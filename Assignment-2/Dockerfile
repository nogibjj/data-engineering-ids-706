# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.245.2/containers/python-3/.devcontainer/base.Dockerfile

FROM peterdavehello/azcopy

RUN apk update && apk add bash && apk add sudo
RUN mkdir app
COPY data_processing_script.sh app/data_processing_script.sh
COPY airtravel.csv app/airtravel.csv
WORKDIR app

ENTRYPOINT ["bash", "data_processing_script.sh"]
