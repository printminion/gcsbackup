https://github.com/GoogleCloudPlatform/storage-file-transfer-json-python

#Install
    git clone git@github.com:printminion/gcsbackup.git && cd gcsbackup
    pip install -r requirements.txt
    
enable  Google Cloud Storage JSON API
https://cloud.google.com/storage/docs/json_api/#activating

Create OAuth 2.0 client ID and get your client secret (example with Google Analytics API Project)
https://www.youtube.com/watch?v=o50lrTq9DjQ
and copy it to client_secrets.json file


#upload
    python gcsbackup.py upload ~/pics/20160723-142358.jpg gs://<bucket_id>/data/2016/07/23/20160723-142358.jpg
    python gcsbackup.py copy gs://<bucket_id>/data/2016/07/25/20160725-000025.jpg gs://<bucket_id>/last.jpg
    python gcsbackup.py download gs://bucket/object ~/Desktop/filename
    python gcsbackup.py predefinedAcl publicRead gs://bucket/object

equivalent to
    
    gsutil cp -p gs://<bucket_id>/data/2016/07/25/20160725-000025.jpg gs://<bucket_id>/last.jpg
    
    gsutil acl ch -u AllUsers:R gs://<bucket_id>/last.jpg
