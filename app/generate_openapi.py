import json
import sys
from fastapi.openapi.utils import get_openapi

from app.main import app


def export_openapi_specification():
    """
    Safely reads application endpoints configuration structures and compiles
    the mandatory docs.json schema specification file entirely offline.
    """
    print("[Doc Utility] Compiling application schema map layout...")

    # Explicitly compile the specification using FastAPI internal mapping tools.
    # This prevents request dependencies from trying to open active network sockets.
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version="3.0.2",
        description=app.description,
        routes=app.routes,
    )

    # Save the synchronized structural blueprint to the root folder
    output_filename = "docs.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(openapi_schema, f, indent=2, ensure_ascii=False)

    print(f"[Doc Utility] Success. Specifications contract saved to: './{output_filename}'")


if __name__ == "__main__":
    try:
        export_openapi_specification()
    except Exception as err:
        print(f"[Doc Utility] Fatal compilation fault: {err}", file=sys.stderr)
        sys.exit(1)
