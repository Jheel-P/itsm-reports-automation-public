gcloud run deploy servicenow-table-sync --region={GCP_REGION} \
    --image=gcr.io/$1/servicenow-table-sync:$2 \
    --concurrency=10 \
    --cpu=1 --memory=512Mi \
    --max-instances=10 --min-instances=0 \
    --port=8080 --timeout="5m" \
    --service-account=SA_EMAIL \
    --no-use-http2 --no-allow-unauthenticated \
    --set-env-vars=CONFIG_KEY=reports-automation/servicenow-table-sync-config \
    --update-labels=LABELS \
    --project={DEPLOYMENT_PROJECT}