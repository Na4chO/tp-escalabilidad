FROM python:3.12-slim
RUN pip install --no-cache-dir pyarrow==21.0.0 nltk==3.9.1 \
    && python -m nltk.downloader -d /usr/share/nltk_data stopwords
COPY expected.py /expected.py
ENTRYPOINT ["python", "/expected.py"]
