FROM python:3.11-slim

WORKDIR /home/jovyan/work

COPY jupyter/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --create-home --uid 1000 jovyan \
    && chown -R jovyan:jovyan /home/jovyan

USER jovyan

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
