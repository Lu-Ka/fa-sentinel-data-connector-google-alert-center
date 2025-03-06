import json

import google.auth.transport.requests

from google.oauth2 import service_account
from googleapiclient.discovery import build


class Google:
    def __init__(self, service_account_info, scope, subject):
        self._load_creds(service_account_info, scope, subject)

    def _load_creds(self, service_account_info, scope, subject):
        sa_info = json.loads(service_account_info)
        self.creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=scope, subject=subject
        )

        request = google.auth.transport.requests.Request()
        self.creds.refresh(request)

        return

    def get_alerts(self, start_time, end_time):
        all_alerts = []

        service = build("alertcenter", "v1beta1", credentials=self.creds)

        filter = f'createTime >= "{start_time}" AND createTime < "{end_time}"'
        request = service.alerts().list(pageSize=10, filter=filter)
        response = request.execute()

        while response is not None:
            for item in response.get("alerts", []):
                all_alerts.append(item)

            request = service.alerts().list_next(
                previous_request=request, previous_response=response
            )

            if request is not None:
                response = request.execute()
            else:
                response = None

        return all_alerts
