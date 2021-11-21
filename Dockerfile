FROM tiangolo/uvicorn-gunicorn:python3.8

WORKDIR /query_optimization

COPY ./app /query_optimization/app
COPY main.py /query_optimization
COPY requirements.txt /query_optimization
#RUN apt-get update &&\
#    apt-get install -y binutils libproj-dev gdal-bin python3-gdal
#
#ARG CPLUS_INCLUDE_PATH=/usr/include/gdal
#ARG C_INCLUDE_PATH=/usr/include/gdal

RUN pip install -r requirements.txt


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80","--timeout-keep-alive", "8000"]
#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "443","--timeout-keep-alive", "6000", "--ssl-keyfile=/fast-map-api/app/certificates/fastmap.key", "--ssl-certfile=/fast-map-api/app/certificates/fastmap.pem"]
#RUN python manage.py loaddata admin/cog/fixtures/asset_info.json
