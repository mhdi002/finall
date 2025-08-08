from app import db
from app.models import Log
from flask_login import current_user

def record_log(action, details=None):
    """
    Helper function to create a log entry.
    """
    try:
        if current_user.is_authenticated:
            log_entry = Log(
                user_id=current_user.id,
                action=action,
                details=details
            )
            db.session.add(log_entry)
            db.session.commit()
    except Exception as e:
        # In a production app, you'd want more robust error handling here,
        # e.g., logging to a file so that log failures don't crash the app.
        print(f"Error recording log: {e}")
