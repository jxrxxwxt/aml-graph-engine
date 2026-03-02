from pydantic import BaseModel, Field, field_validator

class EMVCoPayload(BaseModel):
    """
    Data model representing an inbound EMVCo QR code payload.
    """
    qr_payload: str = Field(..., description="The raw EMVCo QR code string")

    @field_validator('qr_payload')
    @classmethod
    def validate_emvco_format(cls, v: str) -> str:
        """
        Validates the strict format of an EMVCo QR string.
        Hard-drops the payload if the format does not comply.
        """
        # Payload Format Indicator (ID 00) must be "01" (length 02) -> "000201"
        if not v.startswith("000201"):
            raise ValueError("Invalid EMVCo format: Payload Format Indicator missing or incorrect")
        
        # Check for Cyclic Redundancy Check (CRC) field (ID 63, length 04)
        if "6304" not in v:
            raise ValueError("Invalid EMVCo format: CRC component missing")
        
        return v