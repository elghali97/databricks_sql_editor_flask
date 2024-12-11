#!env python3
""" Flask with OAuth:

This application provides end-to-end demonstration of Databricks SDK for Python
capabilities of OAuth Authorization Code flow with PKCE security enabled. This
can help you build a hosted app with every user using their own identity to
access Databricks resources.

If you have already Custom App:

./flask_app_with_oauth.py --host <databricks workspace url> \
    --client_id <app-client-id> \
    --client_secret <app-secret> \
    --warehouse-id
    --port 5001

If you want this script to register Custom App and redirect URL for you:

./flask_app_with_oauth.py --port 5001 --profile <databricks account profile>

You'll get prompted for Databricks Account username and password for
script to enroll your account into OAuth and create a custom app with
http://localhost:5001/callback as the redirect callback. Client and
secret credentials for this OAuth app will be printed to the console,
so that you could resume testing this app at a later stage.

Once started, please open http://localhost:5001 in your browser and
go through SSO flow to get a list of clusters on <databricks workspace url>.
"""

import argparse
import logging
import sys
import pandas as pd 

from databricks.sdk.oauth import OAuthClient
from flask import (Flask, redirect, render_template_string, render_template, request,
                    session, url_for, flash)

APP_NAME = "flask-demo"
warehouse_id = ""


def create_flask_app(oauth_client: OAuthClient):
    """The create_flask_app function creates a Flask app that is enabled with OAuth.

    It initializes the app and web session secret keys with a randomly generated token. It defines two routes for
    handling the callback and index pages.
    """
    import secrets
    from forms import QueryForm
    from flask import (Flask, redirect, render_template_string, render_template, request,
                       session, url_for, flash)

    app = Flask(APP_NAME)
    app.secret_key = secrets.token_urlsafe(32)

    @app.route("/callback")
    def callback():
        """The callback route initiates consent using the OAuth client, exchanges
        the callback parameters, and redirects the user to the index page."""
        from databricks.sdk.oauth import Consent

        consent = Consent.from_dict(oauth_client, session["consent"])
        session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
        return redirect(url_for("index"))

    @app.route('/', methods=('GET', 'POST'))
    def index():
        """The index page checks if the user has already authenticated and retrieves the user's credentials using
        the Databricks SDK WorkspaceClient. It then renders the template with the clusters' list."""
        if "creds" not in session:
            consent = oauth_client.initiate_consent()
            session["consent"] = consent.as_dict()
            return redirect(consent.auth_url)

        from databricks.sdk import WorkspaceClient
        from databricks.sdk.oauth import SessionCredentials
        from databricks.sdk.service import compute

        credentials_provider = SessionCredentials.from_dict(oauth_client, session["creds"])
        workspace_client = WorkspaceClient(host=oauth_client.host,
                                           product=APP_NAME,
                                           credentials_provider=credentials_provider,
                                           )

        form = QueryForm()
        if form.validate_on_submit():
            query = form.sql.data
            try:
                text_results = workspace_client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=query
                )

                columns = [c.name for c in text_results.manifest.schema.columns]
                
                df = pd.DataFrame(data = text_results.result.data_array, columns = columns )
            except Exception as e:
                flash('{}'.format(e), 'alert-danger')
            else:
                return render_template(
                    'index.html',
                    form=form,
                    table=df.to_html(classes='table table-striped table-bordered',
                                    header='true', index=False, justify="left",
                                    show_dimensions=True)
                )
        return render_template('index.html', form=form)

    return app


def init_oauth_config(args) -> OAuthClient:
    """Creates Databricks SDK configuration for OAuth"""
    oauth_client = OAuthClient(host=args.host,
                               client_id=args.client_id,
                               client_secret=args.client_secret,
                               redirect_url=f"http://localhost:{args.port}/callback",
                               scopes=["all-apis"],
                               )
    return oauth_client


def parse_arguments() -> argparse.Namespace:
    """Parses arguments for this demo"""
    parser = argparse.ArgumentParser(prog=APP_NAME, description=__doc__.strip())
    parser.add_argument("--host")
    for flag in ["client_id", "client_secret","warehouse_id"]:
        parser.add_argument(f"--{flag}")
    parser.add_argument("--port", default=5001, type=int)
    parser.add_argument("--profile", default="DEFAULT", help="Databricks account profile to use for authentication.")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO,
                        format="%(asctime)s [%(name)s][%(levelname)s] %(message)s",
                        )
    logging.getLogger("databricks.sdk").setLevel(logging.DEBUG)

    args = parse_arguments()
    oauth_cfg = init_oauth_config(args)
    warehouse_id = args.warehouse_id
    app = create_flask_app(oauth_cfg)

    app.run(
        host="localhost",
        port=args.port,
        debug=True,
        # to simplify this demo experience, we create OAuth Custom App for you,
        # but it intervenes with the werkzeug reloader. So we disable it
        use_reloader=args.client_id is not None,
    )