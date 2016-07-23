__author__ = 'm.kupriyanov'

import httplib2

from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage


class Auth:
    google_service = None

    def __init__(self, google_service):
        self.google_service = google_service

    @staticmethod
    def create_service_account(user_email, scope, SERVICE_ACCOUNT_PKCS12_FILE_PATH, SERVICE_ACCOUNT_EMAIL):
        """Build and returns a Drive service object authorized with the service accounts
        that act on behalf of the given user.

        Args:
        user_email: The email of the user.
        scope: The email of the user.
        SERVICE_ACCOUNT_PKCS12_FILE_PATH: /path/to/<public_key_fingerprint>-privatekey.p12
        SERVICE_ACCOUNT_EMAIL: <some-id>@developer.gserviceaccount.com

        Returns:
        Drive service object.
        """
        f = file(SERVICE_ACCOUNT_PKCS12_FILE_PATH, 'rb')
        key = f.read()
        f.close()

        credentials = SignedJwtAssertionCredentials(
            service_account_name=SERVICE_ACCOUNT_EMAIL,
            private_key=key,
            scope=scope,
            sub=user_email,
        )
        http = httplib2.Http()
        http = credentials.authorize(http)

        return http

    @staticmethod
    def create_service(scope, CLIENT_CREDENTIALS_FILE, CLIENT_SECRET_FILE):

        # Run through the OAuth flow and retrieve credentials
        flow = None
        try:
            flow = flow_from_clientsecrets(CLIENT_SECRET_FILE, scope=scope, redirect_uri="urn:ietf:wg:oauth:2.0:oob")
        except Exception, e:
            flow = flow_from_clientsecrets(CLIENT_SECRET_FILE, scope=scope)

        storage = Storage(CLIENT_CREDENTIALS_FILE)
        credentials = storage.get()

        if credentials is None:
            authorize_url = flow.step1_get_authorize_url()
            print 'Go to the following link in your browser: ' + authorize_url
            code = raw_input('Enter verification code: ').strip()

            credentials = flow.step2_exchange(code)

            # save credentials
            storage.put(credentials)

        # Create an httplib2.Http object and authorize it with our credentials
        http = httplib2.Http()
        http = credentials.authorize(http)

        return http
