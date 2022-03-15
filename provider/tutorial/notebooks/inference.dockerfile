FROM mcr.microsoft.com/azureml/openmpi3.1.2-ubuntu18.04:20210615.v1

ENV AZUREML_CONDA_ENVIRONMENT_PATH /azureml-envs/feast

RUN conda create -p $AZUREML_CONDA_ENVIRONMENT_PATH \
    python=3.8 pip=20.2.4

ENV PATH $AZUREML_CONDA_ENVIRONMENT_PATH/bin:$PATH

RUN apt-get install -y gcc
RUN apt-get install -y unixodbc-dev

RUN pip install 'azureml-defaults==1.35.0' \
                'feast-azure-provider==0.3.0' \
                'scikit-learn==0.22.2.post1' \
		        'joblib===1.1.0' \
                'itsdangerous==2.0.1'