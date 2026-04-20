"""Auto-load `.env` at entry points.

Both `sap-mining` (CLI) and the Streamlit app import this module. It walks up from the
current working directory looking for a `.env` file and loads it into `os.environ`
without overriding vars already set in the shell — so explicit exports still win.

Import for side-effect only:

    from sap_process_mining import _env  # noqa: F401
"""

from __future__ import annotations

from dotenv import find_dotenv, load_dotenv

# `usecwd=True` searches from the current working directory upward. `override=False`
# means a real `export ANTHROPIC_API_KEY=…` still takes precedence over the file.
load_dotenv(find_dotenv(usecwd=True), override=False)
