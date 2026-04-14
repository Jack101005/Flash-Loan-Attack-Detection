# Backend Setup and Execution Guide

This document provides instructions on how to set up and run the FastAPI backend for the Flash-Loan Attack Detection system.

## Prerequisites
- Python 3.9 or higher

## 1. Create a Virtual Environment
It is highly recommended to use a virtual environment to manage dependencies and avoid conflicts with other Python projects.

Run the following command inside the `backend` directory:

```bash
python -m venv venv
```

## 2. Activate the Virtual Environment
Before installing dependencies or running the server, you must activate the virtual environment.

- **On Windows (PowerShell):**
  ```powershell
  .\venv\Scripts\activate
  ```

- **On Windows (Command Prompt):**
  ```cmd
  venv\Scripts\activate.bat
  ```

- **On MacOS / Linux:**
  ```bash
  source venv/bin/activate
  ```

## 3. Install Dependencies
With the virtual environment activated, install the required packages:

```bash
pip install -r Requirements.txt
```

*(If the requirements file is missing the specific ASGI server, you may also need to run: `pip install fastapi uvicorn pydantic`)*

## 4. Run the Server
Start the FastAPI server using Uvicorn with hot-reloading enabled for development:

```bash
python -m uvicorn Main:app --reload --port 8000
```

The server will start and listen on `http://127.0.0.1:8000`.

## 5. API Documentation
Once the server is running, you can access the automatic interactive API documentation provided by FastAPI:

- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`

## Stopping the Server
To stop the server, press `CTRL + C` in your terminal. You can then deactivate the virtual environment by typing:

```bash
deactivate
```
