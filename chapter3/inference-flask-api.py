#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, subprocess, json
from flask import Flask, request

app = Flask(__name__)

uploads_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads/")
if not os.path.exists(uploads_folder):
    os.makedirs(uploads_folder)


@app.route("/infer", methods=["POST"])
def infer():
    if "file" in request.files:
        print("'file' found.")
        f = request.files["file"]
        fpath = os.path.join(uploads_folder, f.filename) 
        f.save(fpath)
        print("File '%s' saved to '%s'" % (f.filename, fpath))
        result = subprocess.run(["/wmlce/data/run_demo.sh", fpath],
                stdout=subprocess.PIPE)
        os.remove(fpath)
        return  json.loads(result.stdout.decode("utf-8"))
    else:
        return {"answer": "No 'file' provided."}

if __name__=="__main__":
    app.run(host="0.0.0.0", port=5000)
