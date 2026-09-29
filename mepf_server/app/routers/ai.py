# -*- coding: utf-8 -*-
"""
/api/ai - AI preprocessing endpoints.

POST /api/ai/generate-and-place runs the full pipeline described in
AI_PREPROCESSING_DESIGN.md: reads rooms from the target document, asks
Claude to decide what MEP elements belong in each one (constrained to an
allowed catalog), validates every decision against the actual room data,
and creates a normal place_mep_elements command from what survives
validation. The response reports every stage so nothing is a black box.
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session

from app.auth import require_api_key
from app.models.db import get_db
from app.orchestrator.ai_preprocessor import generate_and_place, AIPreprocessingError

router = APIRouter(prefix="/api/ai", tags=["ai"], dependencies=[Depends(require_api_key)])


class GenerateAndPlaceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_requirements: dict = Field(default_factory=dict)
    target_selector: dict = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=10)


@router.post("/generate-and-place")
def generate_and_place_endpoint(body: GenerateAndPlaceRequest, db: Session = Depends(get_db)):
    try:
        return generate_and_place(
            db,
            body.project_requirements,
            body.target_selector,
            max_attempts=body.max_attempts,
        )
    except AIPreprocessingError as ex:
        raise HTTPException(
            status_code=422,
            detail={"code": ex.code, "message": ex.message, "detail": ex.detail},
        )
