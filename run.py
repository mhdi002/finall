from app import create_app, db
from app.models import User, Role

app = create_app()

@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'User': User, 'Role': Role}

def setup_initial_roles():
    """Initializes roles if the roles table is empty."""
    with app.app_context():
        # This check prevents re-creating roles on every app start
        if Role.query.count() == 0:
            print("No roles found. Initializing roles...")
            roles = ['Viewer', 'Admin', 'Owner']
            for r in roles:
                role = Role(name=r)
                db.session.add(role)
            db.session.commit()
            print("Roles successfully initialized.")

if __name__ == '__main__':
    # Set up roles before the first request
    setup_initial_roles()
    # When running with a production WSGI server like Gunicorn,
    # this block is not executed. The port and host are configured
    # in the WSGI server's command.
    # For `flask run`, the host and port are specified in the Dockerfile's CMD.
    app.run(host='0.0.0.0', port=5001, debug=False)
