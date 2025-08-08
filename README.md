# Financial Report Web Application


[![Python application](https://github.com/mhdi002/111/actions/workflows/main.yml/badge.svg)](https://github.com/mhdi002/111/actions/workflows/main.yml)

This is a comprehensive web application for processing, analyzing, and visualizing financial deal data, built with Flask and containerized with Docker.

## Features

- **Secure Authentication**: Robust user registration and login system with strong password requirements.
- **Role-Based Access Control**: Three user roles (Viewer, Admin, Owner) with distinct permissions.
- **File Uploads**: Securely upload CSV files for deals, excluded accounts, and VIP clients.
- **Advanced Data Processing**: A powerful backend that processes the data, splits it into A/B/Multi books, and performs complex financial calculations, based on the logic from the original `report.py` script.
- **Stage 2 - Advanced Reporting**: A feature for uploading and processing various financial reports (CSV/XLSX), which are then stored in the database. The application can then generate reports and perform analysis on this data, including discrepancy checks.
- **Interactive Dashboard**: A clean, tabbed interface for viewing results, including summary tables and dynamic charts generated with Plotly.
- **Audit Logging**: All key user actions (logins, uploads, etc.) are logged and can be viewed by the site Owner in an admin panel.
- **Containerized Deployment**: A complete Dockerfile allows for easy, consistent deployment on any machine.
- **Automated Testing & CI/CD**: A GitHub Actions workflow automatically lints and tests the code on every push and pull request.

## Project Structure

```
/
├── app/                  # Main Flask application package
│   ├── __init__.py       # Application factory
│   ├── routes.py         # Application routes
│   ├── models.py         # SQLAlchemy database models
│   ├── forms.py          # WTForms classes
│   ├── processing.py     # Core data processing logic
│   ├── stage2_processing.py # Stage 2 data processing
│   ├── stage2_reports.py # Stage 2 reporting logic
│   ├── logger.py         # Audit logging helper
│   ├── charts.py         # Chart generation logic
│   ├── static/           # Static files (CSS, JS)
│   └── templates/        # Jinja2 HTML templates
├── instance/             # Instance-specific data (DB, uploads)
├── migrations/           # Flask-Migrate migration scripts
├── tests/                # Unit and integration tests
├── .github/workflows/    # CI/CD workflow definitions
│   └── main.yml
├── config.py             # Application configuration
├── run.py                # Application entry point
├── requirements.txt      # Python dependencies
├── Dockerfile.txt        # Docker container definition
└── README.md             # This file
```

---

## Local Development Setup

### Prerequisites
- Python 3.10+
- `pip` and `venv`

### 1. Set up Virtual Environment
Create and activate a virtual environment in the project root:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### 2. Install Dependencies
Install all required packages:
```bash
pip install -r requirements.txt
```

### 3. Set up Environment Variables
The application uses a `.flaskenv` file to manage environment variables for development. This file is already included in the repository. It sets `FLASK_APP` and `FLASK_ENV`.

### 4. Initialize the Database
The first time you set up the project, you need to initialize the database and apply the migrations:
```bash
# Make sure your FLASK_APP is set (done by the .flaskenv file)
flask db upgrade
```
This will create the `instance/app.db` SQLite file and all the necessary tables (users, roles, logs).

### 5. Run the Application
Start the Flask development server:
```bash
flask run
```
The application will be available at `http://127.0.0.1:5000`. The `setup_initial_roles` function in `run.py` will automatically populate the 'Viewer', 'Admin', and 'Owner' roles on the first run.

---

## Running the Tests

To run the comprehensive test suite:
```bash
python -m unittest discover tests
```

---

## Deployment on a Linux Server (Production)

This guide explains how to deploy the application on a Linux server using Docker and Gunicorn.

### Prerequisites
- A Linux server with Docker and Docker Compose installed.
- Git installed on the server.

### 1. Clone the Repository
Clone the project repository to your server:
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Configure Environment Variables
Create a `.env` file in the project root to store your production environment variables. At a minimum, you should set:
```
FLASK_APP=run.py
SECRET_KEY=<your-very-secret-key>
SQLALCHEMY_DATABASE_URI=sqlite:///instance/app.db
```
**Note:** For a real production environment, you should use a more robust database like PostgreSQL or MySQL and set the `SQLALCHEMY_DATABASE_URI` accordingly.

### 3. Build and Run the Docker Container
The provided `Dockerfile.txt` is configured to use Gunicorn as the production WSGI server. Build and run the container in detached mode:

```bash
# Build the Docker image
sudo docker build -f Dockerfile.txt -t report-app .

# Run the container
sudo docker run -d \
  -p 80:5000 \
  --name report-app-container \
  -v $(pwd)/instance:/app/instance \
  --env-file .env \
  report-app
```

**Explanation of the `docker run` command:**
- `-d`: Runs the container in detached mode (in the background).
- `-p 80:5000`: Maps port 80 on the host to port 5000 in the container. This allows you to access the application directly via the server's IP address without specifying a port.
- `--name report-app-container`: Assigns a name to the container for easy management.
- `-v $(pwd)/instance:/app/instance`: Mounts the `instance` directory from the host to the container. This ensures that your database and uploaded files persist even if the container is removed and recreated.
- `--env-file .env`: Loads the environment variables from the `.env` file you created.
- `report-app`: The name of the image to run.

### 4. Initial Database Setup (First-Time Deployment)
On the first run, you need to initialize the database inside the container:
```bash
sudo docker exec -it report-app-container flask db upgrade
```

### 5. Accessing the Application
The application should now be accessible at your server's IP address or domain name (e.g., `http://<your-server-ip>`).

### Managing the Container
- **View logs**: `sudo docker logs report-app-container`
- **Stop the container**: `sudo docker stop report-app-container`
- **Start the container**: `sudo docker start report-app-container`
- **Remove the container**: `sudo docker rm report-app-container`
