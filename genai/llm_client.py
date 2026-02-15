import os

class LLMClient:
    def __init__(self):
        self.provider = os.getenv("LLM_PROVIDER", "mock")

    def generate(self, prompt: str) -> str:
        if self.provider == "mock":
            return self._mock_response(prompt)
        else:
            raise NotImplementedError(
                f"Provider '{self.provider}' not implemented in this environment."
            )

    def _mock_response(self, prompt: str) -> str:
        """
        Deterministic mock LLM response.
        Simulates reasoning based on retrieved anomaly text.
        """

        if "EC2" in prompt:
            return (
                "The cost increase is likely driven by higher EC2 compute usage. "
                "This may indicate additional instances launched, increased runtime, "
                "or migration to larger instance types."
            )

        if "S3" in prompt:
            return (
                "The cost increase appears to be related to higher S3 storage or "
                "data transfer usage. This could be caused by increased object storage "
                "or cross-region transfer activity."
            )

        return (
            "The cost change appears linked to increased service consumption. "
            "Further breakdown by service and usage type is recommended."
        )

