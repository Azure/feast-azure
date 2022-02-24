# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas
import pyarrow as pa
from tqdm import tqdm

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.online_stores.helpers import get_online_store_from_config
from feast.infra.provider import (
    Provider,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset

DEFAULT_BATCH_SIZE = 10_000

class AzureProvider(Provider):
    def __init__(self, config: RepoConfig):
        self.repo_config = config
        self.offline_store = get_offline_store_from_config(config.offline_store)
        self.online_store = (
            get_online_store_from_config(config.online_store)
            if config.online_store
            else None
        )

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        # Call update only if there is an online store
        if self.online_store:
            self.online_store.update(
                config=self.repo_config,
                tables_to_delete=tables_to_delete,
                tables_to_keep=tables_to_keep,
                entities_to_keep=entities_to_keep,
                entities_to_delete=entities_to_delete,
                partial=partial,
            )

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        if self.online_store:
            self.online_store.teardown(self.repo_config, tables, entities)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if self.online_store:
            self.online_store.online_write_batch(config, table, data, progress)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        result = []
        if self.online_store:
            result = self.online_store.online_read(config, table, entity_keys, requested_features)
        return result

    def ingest_df(
        self, feature_view: FeatureView, entities: List[Entity], df: pandas.DataFrame,
    ):
        table = pa.Table.from_pandas(df)

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        join_keys = {entity.join_key: entity.value_type for entity in entities}
        rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        self.online_write_batch(
            self.repo_config, feature_view, rows_to_write, progress=None
        )

    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            event_timestamp_column,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        offline_job = self.offline_store.pull_latest_from_table_or_query(
            config=config,
            data_source=feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        table = offline_job.to_arrow()

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        join_keys = {entity.join_key: entity.value_type for entity in entities}
        
        with tqdm_builder(table.num_rows) as pbar:
            for batch in table.to_batches(DEFAULT_BATCH_SIZE):
                rows_to_write = _convert_arrow_to_proto(batch, feature_view, join_keys)
                self.online_write_batch(
                    self.repo_config,
                    feature_view,
                    rows_to_write,
                    lambda x: pbar.update(x),
                )

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool,
    ) -> RetrievalJob:
        job = self.offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
        )
        return job

    def retrieve_saved_dataset(
        self,
        config: RepoConfig,
        dataset: SavedDataset
    ) -> RetrievalJob:
        feature_name_columns = [
            ref.replace(":", "__") if dataset.full_feature_names else ref.split(":")[1]
            for ref in dataset.features
        ]

        # ToDo: replace hardcoded value
        event_ts_column = "event_timestamp"

        return self.offline_store.pull_all_from_table_or_query(
            config=config,
            data_source=dataset.storage.to_data_source(),
            join_key_columns=dataset.join_keys,
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_ts_column,
            start_date=make_tzaware(dataset.min_event_timestamp),  # type: ignore
            end_date=make_tzaware(dataset.max_event_timestamp + timedelta(seconds=1)),  # type: ignore
        )

