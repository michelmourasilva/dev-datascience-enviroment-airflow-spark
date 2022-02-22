FROM bitnami/python:3.9.7 as build

FROM bitnami/spark:3.2.0

COPY --from=build /opt/bitnami/python/ /opt/bitnami/python/
