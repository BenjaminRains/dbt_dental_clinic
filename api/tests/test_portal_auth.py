"""Tests for clinic portal login."""

import json
import os

import pytest
from fastapi.testclient import TestClient

from auth.portal import create_session_token, verify_session_token, PortalUser


@pytest.fixture
def portal_env(monkeypatch):
    users = [
        {
            "username": "owner",
            "password": "owner-pass",
            "role": "owner",
            "display_name": "Practice Owner",
        },
        {
            "username": "manager",
            "password": "mgr-pass",
            "role": "practice-manager",
            "display_name": "Practice Manager",
        },
    ]
    monkeypatch.setenv("API_ENVIRONMENT", "clinic")
    monkeypatch.setenv("CLINIC_PORTAL_USERS", json.dumps(users))
    monkeypatch.delenv("CLINIC_PORTAL_USERS_FILE", raising=False)
    monkeypatch.setattr(
        "auth.portal.DEFAULT_PORTAL_USERS_FILE",
        __import__("pathlib").Path("/nonexistent/clinic-portal-users.json"),
    )
    monkeypatch.setenv("CLINIC_PORTAL_SESSION_SECRET", "test-portal-secret")
    monkeypatch.setenv("CLINIC_API_KEY", "test-api-key")


def test_create_and_verify_session_token(portal_env):
    user = PortalUser(
        username="owner",
        password="x",
        role="owner",
        display_name="Practice Owner",
    )
    token = create_session_token(user)
    session = verify_session_token(token)
    assert session.username == "owner"
    assert session.role == "owner"
    assert session.display_name == "Practice Owner"


def test_portal_login_success(portal_env):
    from main import app

    client = TestClient(app)
    response = client.post(
        "/auth/login",
        json={"username": "owner", "password": "owner-pass"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["role"] == "owner"
    assert data["display_name"] == "Practice Owner"
    assert "token" in data

    me = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {data['token']}"},
    )
    assert me.status_code == 200
    assert me.json()["username"] == "owner"


def test_portal_admin_role_accepted(monkeypatch):
    users = [
        {
            "username": "admin",
            "password": "admin-pass",
            "role": "admin",
            "display_name": "Clinic Admin",
        }
    ]
    monkeypatch.setenv("API_ENVIRONMENT", "clinic")
    monkeypatch.setenv("CLINIC_PORTAL_USERS", json.dumps(users))
    monkeypatch.delenv("CLINIC_PORTAL_USERS_FILE", raising=False)
    monkeypatch.setattr(
        "auth.portal.DEFAULT_PORTAL_USERS_FILE",
        __import__("pathlib").Path("/nonexistent/clinic-portal-users.json"),
    )
    monkeypatch.setenv("CLINIC_PORTAL_SESSION_SECRET", "test-portal-secret")
    monkeypatch.setenv("CLINIC_API_KEY", "test-api-key")

    from main import app

    client = TestClient(app)
    response = client.post(
        "/auth/login",
        json={"username": "admin", "password": "admin-pass"},
    )
    assert response.status_code == 200
    assert response.json()["role"] == "admin"


def test_portal_login_invalid_password(portal_env):
    from main import app

    client = TestClient(app)
    response = client.post(
        "/auth/login",
        json={"username": "owner", "password": "wrong"},
    )
    assert response.status_code == 401


def test_load_users_from_json_file(tmp_path, monkeypatch):
    users_file = tmp_path / "users.json"
    users_file.write_text(
        json.dumps(
            [
                {
                    "username": "owner",
                    "password": "secret",
                    "role": "owner",
                    "display_name": "Owner",
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("API_ENVIRONMENT", "clinic")
    monkeypatch.setenv("CLINIC_PORTAL_USERS_FILE", str(users_file))
    monkeypatch.delenv("CLINIC_PORTAL_USERS", raising=False)

    from auth.portal import load_portal_users

    users = load_portal_users()
    assert len(users) == 1
    assert users[0].username == "owner"


def test_portal_login_not_configured(monkeypatch):
    monkeypatch.setenv("API_ENVIRONMENT", "clinic")
    monkeypatch.delenv("CLINIC_PORTAL_USERS", raising=False)
    monkeypatch.delenv("CLINIC_PORTAL_USERS_FILE", raising=False)
    monkeypatch.setenv("CLINIC_API_KEY", "test-api-key")
    monkeypatch.setenv("CLINIC_PORTAL_SESSION_SECRET", "test-portal-secret")
    # Ensure default file is not picked up if present in dev tree
    monkeypatch.setattr(
        "auth.portal.DEFAULT_PORTAL_USERS_FILE",
        __import__("pathlib").Path("/nonexistent/clinic-portal-users.json"),
    )

    from main import app

    client = TestClient(app)
    response = client.post(
        "/auth/login",
        json={"username": "owner", "password": "owner-pass"},
    )
    assert response.status_code == 503
