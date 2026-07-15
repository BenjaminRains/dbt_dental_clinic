"""Unit tests for frontend SPA route key split (Phase 0 of FRONTEND_SPLIT_PLAN)."""

import pytest

from mdc_cli.deploy_frontend import (
    CLINIC_SPA_ROUTE_KEYS,
    PORTFOLIO_SPA_ROUTE_KEYS,
    spa_route_keys_for_target,
)


def test_portfolio_keys_include_showcase_not_clinic_homes():
    assert "agent-profile" in PORTFOLIO_SPA_ROUTE_KEYS
    assert "environment-manager" in PORTFOLIO_SPA_ROUTE_KEYS
    assert "schema-discovery" in PORTFOLIO_SPA_ROUTE_KEYS
    assert "dashboard" in PORTFOLIO_SPA_ROUTE_KEYS
    assert "login" not in PORTFOLIO_SPA_ROUTE_KEYS
    assert "home/owner" not in PORTFOLIO_SPA_ROUTE_KEYS


def test_clinic_keys_include_portal_not_portfolio_showcase():
    assert "login" in CLINIC_SPA_ROUTE_KEYS
    assert "home/admin" in CLINIC_SPA_ROUTE_KEYS
    assert "home/owner" in CLINIC_SPA_ROUTE_KEYS
    assert "home/practice-manager" in CLINIC_SPA_ROUTE_KEYS
    assert "dashboard" in CLINIC_SPA_ROUTE_KEYS
    assert "agent-profile" not in CLINIC_SPA_ROUTE_KEYS
    assert "environment-manager" not in CLINIC_SPA_ROUTE_KEYS
    assert "schema-discovery" not in CLINIC_SPA_ROUTE_KEYS


def test_spa_route_keys_for_target():
    assert spa_route_keys_for_target("demo") == PORTFOLIO_SPA_ROUTE_KEYS
    assert spa_route_keys_for_target("clinic") == CLINIC_SPA_ROUTE_KEYS


def test_spa_route_keys_unknown_target():
    with pytest.raises(ValueError, match="Unknown frontend deploy target"):
        spa_route_keys_for_target("local")
