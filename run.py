# -*- coding: utf-8 -*-

import uvicorn

if __name__ == "__main__":
    uvicorn.run("spotlyt.server:app", host='0.0.0.0', port=5000, reload=True, debug=True, workers=2)
