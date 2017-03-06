import arcpy
import io
import json
import os
import requests
import swiftclient.client as swiftclient
import time
from urllib.parse import urlparse


class Toolbox(object):
    def __init__(self):
        self.alias = "GeoBinToolbox"
        self.label = "GeoBin Toolbox"
        self.tools = [
            UploadTool,
            RemoveTool,
            GeoBinTool
        ]


class UploadTool(object):
    def __init__(self):
        self.label = "Upload GeoBin"
        self.description = "Upload GeoBin"
        self.canRunInBackground = True

    def getParameterInfo(self):
        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        py_path = arcpy.Parameter(
            name="py_path",
            displayName="GeoBin Python Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        py_path.value = os.path.join(os.path.dirname(__file__), "GeoBin.py")

        return [bm_path, py_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[0].valueAsText
        py_path = parameters[1].valueAsText
        head, tail = os.path.split(py_path)
        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            spark = bluemix["spark"]
            with open(py_path, "rb") as data:
                url = "https://spark.bluemix.net/tenant/data/{}/{}".format(spark["instance_id"], tail)
                headers = {
                    "X-Spark-service-instance-id": spark["instance_id"]
                }
                r = requests.put(url,
                                 auth=(spark["tenant_id"], spark["tenant_secret"]),
                                 headers=headers,
                                 data=data
                                 )
                arcpy.AddMessage(json.dumps(r.json()))


class DownloadTool(object):
    def __init__(self):
        self.label = "Download Python File"
        self.description = "Download Python File"
        self.canRunInBackground = True

    def getParameterInfo(self):
        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        py_path = arcpy.Parameter(
            name="py_path",
            displayName="GeoBin Python Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        py_path.value = os.path.join(os.path.dirname(__file__), "GeoBin.py")

        return [bm_path, py_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[0].valueAsText
        py_path = parameters[1].valueAsText
        head, tail = os.path.split(py_path)
        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            spark = bluemix["spark"]
            url = "https://spark.bluemix.net/tenant/data/{}/{}".format(spark["instance_id"], tail)
            headers = {
                "X-Spark-service-instance-id": spark["instance_id"]
            }
            r = requests.get(url,
                             auth=(spark["tenant_id"], spark["tenant_secret"]),
                             headers=headers
                             )
            arcpy.AddMessage(r.text)


class RemoveTool(object):
    def __init__(self):
        self.label = "Remove GeoBin"
        self.description = "Remove GeoBin"
        self.canRunInBackground = True

    def getParameterInfo(self):
        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        return [bm_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[0].valueAsText
        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            spark = bluemix["spark"]
            url = "https://spark.bluemix.net/tenant/data/" + spark["instance_id"]
            headers = {
                "X-Spark-service-instance-id": spark["instance_id"]
            }
            r = requests.delete(url,
                                auth=(spark["tenant_id"], spark["tenant_secret"]),
                                headers=headers
                                )
            resp = r.json()
            if "file_error" in resp:
                arcpy.AddWarning(json.dumps(resp))
            else:
                arcpy.AddMessage(json.dumps(resp))


class GeoBinTool(object):
    def __init__(self):
        self.wait_try = 0
        self.wait_max = 30
        self.wait_sec = 7
        self.running = True
        self.spark = {}
        self.storage = {}
        self.label = "GeoBin Analysis"
        self.description = "GeoBin Analysis"
        self.canRunInBackground = True

    def getParameterInfo(self):
        out_fc = arcpy.Parameter(
            name="out_fc",
            displayName="out_fc",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")
        out_fc.symbology = os.path.join(os.path.dirname(__file__), "GeoBin.lyrx")

        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        input_path = arcpy.Parameter(
            name="input_path",
            displayName="Swift Input Path",
            direction="Input",
            datatype="String",
            parameterType="Required")
        input_path.value = "swift2d://trips.thunder/trips-1M.csv"

        output_path = arcpy.Parameter(
            name="output_path",
            displayName="Swift Output Path",
            direction="Input",
            datatype="String",
            parameterType="Required")
        output_path.value = "swift2d://output.thunder/GeoBins"

        bin_size = arcpy.Parameter(
            name="bin_size",
            displayName="Bin Size",
            direction="Input",
            datatype="String",
            parameterType="Required")
        bin_size.value = "0.001"

        del_folder = arcpy.Parameter(
            name="del_folder",
            displayName="Delete Work Folder",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        del_folder.value = True

        return [out_fc, bm_path, input_path, output_path, bin_size, del_folder]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def check_status(self):
        while self.running and self.wait_try < self.wait_max:
            time.sleep(self.wait_sec)
            self.wait_try += 1
            headers = {"X-Requested-With": "spark-submit"}
            data = {
                "sparkProperties": {
                    "spark.service.tenant_id": self.spark["tenant_id"],
                    "spark.service.instance_id": self.spark["instance_id"],
                    "spark.service.tenant_secret": self.spark["tenant_secret"],
                    "spark.service.spark_version": "2.0"
                }
            }
            arcpy.AddMessage(json.dumps(data))
            r = requests.get("https://spark.bluemix.net/v1/submissions/status/" + self.spark["submissionId"],
                             headers=headers,
                             json=data
                             )
            resp = r.json()
            arcpy.SetProgressorLabel("{} {}".format(resp["driverState"], self.wait_try))
            self.running = resp["success"] and resp["driverState"] == "RUNNING"
            yield resp

    def del_workdir(self):
        url = "https://spark.bluemix.net/tenant/data/workdir/" + self.spark["submissionId"]
        headers = {
            "X-Spark-service-instance-id": self.spark["instance_id"]
        }
        r = requests.delete(url,
                            auth=(self.spark["tenant_id"], self.spark["tenant_secret"]),
                            headers=headers
                            )
        arcpy.AddMessage(json.dumps(r.json()))

    def insert_bins(self, fc, lines):
        with arcpy.da.InsertCursor(fc, ["SHAPE@XY", "POP"]) as cursor:
            for line in lines:
                t = line.decode().rstrip().split(",")
                if len(t) == 3:
                    shape_x = float(t[0])
                    shape_y = float(t[1])
                    pop = int(t[2])
                    cursor.insertRow(((shape_x, shape_y), pop))

    def import_bins(self, parameters):
        url = urlparse(parameters[3].value)
        container, _ = url.netloc.split(".")
        name = url.path[1:]
        part = name + "/part"

        in_memory = True
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        sp_ref = arcpy.SpatialReference(4326)
        arcpy.management.CreateFeatureclass(ws, name, "POINT",
                                            spatial_reference=sp_ref,
                                            has_m="DISABLED",
                                            has_z="DISABLED")
        arcpy.management.AddField(fc, "POP", "LONG")

        arcpy.SetProgressorLabel("Finding Parts...")

        conn = swiftclient.Connection(
            key=self.storage["password"],
            authurl=self.storage["auth_url"] + "/v3",
            auth_version="3",
            os_options={
                "project_id": self.storage["projectId"],
                "user_id": self.storage["userId"],
                "region_name": self.storage["region"]
            })

        for data in conn.get_container(container)[1]:
            object_name = data['name']
            if object_name.startswith(part):
                arcpy.SetProgressorLabel(object_name)
                _, body = conn.get_object(container, object_name)
                self.insert_bins(fc, io.BytesIO(body))

        parameters[0].value = fc

    def execute(self, parameters, messages):
        bm_path = parameters[1].valueAsText
        inp_path = parameters[2].valueAsText
        out_path = parameters[3].valueAsText
        bin_size = parameters[4].valueAsText
        del_work = parameters[5].value

        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            epoch = int(time.time())
            spark = bluemix["spark"]
            storage = bluemix["storage"]
            url = "https://spark.bluemix.net/v1/submissions/create"
            headers = {"X-Requested-With": "spark-submit"}
            data = {
                "action": "CreateSubmissionRequest",
                "appArgs": [
                    "--primary-py-file",
                    "GeoBin.py"
                ],
                "appResource": "{}/GeoBin.py".format(spark["instance_id"]),
                "clientSparkVersion": "2.0",
                "mainClass": "org.apache.spark.deploy.PythonRunner",
                "sparkProperties": {
                    "spark.app.name": "GeoBin{}".format(epoch),
                    "spark.files": "{}/GeoBin.py".format(spark["instance_id"]),
                    "spark.service.spark_version": "2.0",
                    "spark.service.tenant_id": spark["tenant_id"],
                    "spark.service.instance_id": spark["instance_id"],
                    "spark.service.tenant_secret": spark["tenant_secret"],
                    "spark.service.user.fs.swift2d.impl": "com.ibm.stocator.fs.ObjectStoreFileSystem",
                    "spark.service.user.fs.swift2d.service.thunder.auth.method": "keystoneV3",
                    "spark.service.user.fs.swift2d.service.thunder.auth.endpoint.prefix": "endpoints",
                    "spark.service.user.fs.swift2d.service.thunder.auth.url": storage[
                                                                                  "auth_url"] + "/v3/auth/tokens",
                    "spark.service.user.fs.swift2d.service.thunder.region": storage["region"],
                    "spark.service.user.fs.swift2d.service.thunder.tenant": storage["projectId"],
                    "spark.service.user.fs.swift2d.service.thunder.username": storage["userId"],
                    "spark.service.user.fs.swift2d.service.thunder.password": storage["password"],
                    "spark.service.user.fs.swift2d.service.thunder.public": "true",
                    "spark.service.user.input.path": inp_path,
                    "spark.service.user.output.path": out_path,
                    "spark.service.user.cell.size": bin_size
                }
            }
            r = requests.post(url,
                              headers=headers,
                              json=data
                              )
            resp = r.json()
            text = json.dumps(resp)
            if "success" in resp and resp["success"]:
                if False:
                    with open(os.path.join(os.path.dirname(__file__), "submit-res.json"), "w") as f:
                        f.write(text)
                spark["submissionId"] = resp["submissionId"]
                self.spark = spark
                self.storage = storage
                self.running = True
                for resp in self.check_status():
                    arcpy.AddMessage(json.dumps(resp))
                if resp["success"]:
                    self.import_bins(parameters)
                    if del_work:
                        self.del_workdir()
            else:
                arcpy.AddError(text)


class StatusTool(object):
    def __init__(self):
        self.label = "GeoBin Status"
        self.description = "GeoBin Status"
        self.canRunInBackground = True

    def getParameterInfo(self):
        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        submit_path = arcpy.Parameter(
            name="submit_path",
            displayName="Submit File",
            direction="Input",
            datatype="File",
            parameterType="Required")
        submit_path.value = os.path.join(os.path.dirname(__file__), "submit-res.json")

        return [bm_path, submit_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[0].valueAsText
        submit_path = parameters[1].valueAsText
        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            spark = bluemix["spark"]
            with open(submit_path) as submit_file:
                submit = json.load(submit_file)
                headers = {"X-Requested-With": "spark-submit"}
                data = {
                    "sparkProperties": {
                        "spark.service.tenant_id": spark["tenant_id"],
                        "spark.service.instance_id": spark["instance_id"],
                        "spark.service.tenant_secret": spark["tenant_secret"],
                        "spark.service.spark_version": "2.0"
                    }
                }
                r = requests.get("https://spark.bluemix.net/v1/submissions/status/" + submit["submissionId"],
                                 headers=headers,
                                 json=data
                                 )
                text = json.dumps(r.json())
                arcpy.AddMessage(text)
                with open(os.path.join(os.path.dirname(__file__), "status.json"), "w") as f:
                    f.write(text)


class DeleteWorkdirTool(object):
    def __init__(self):
        self.label = "Delete Work Dir"
        self.description = "Delete Work Dir"
        self.canRunInBackground = True

    def getParameterInfo(self):
        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        submit_path = arcpy.Parameter(
            name="submit_path",
            displayName="Submit File",
            direction="Input",
            datatype="File",
            parameterType="Required")
        submit_path.value = os.path.join(os.path.dirname(__file__), "submit-res.json")

        return [bm_path, submit_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[0].valueAsText
        submit_path = parameters[1].valueAsText
        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            spark = bluemix["spark"]
            with open(submit_path) as submit_file:
                submit = json.load(submit_file)
                url = "https://spark.bluemix.net/tenant/data/workdir/" + submit["submissionId"]
                headers = {
                    "X-Spark-service-instance-id": spark["instance_id"]
                }
                r = requests.delete(url,
                                    auth=(spark["tenant_id"], spark["tenant_secret"]),
                                    headers=headers
                                    )
                resp = r.json()
                if "file_error" in resp:
                    arcpy.AddWarning(json.dumps(resp))
                else:
                    arcpy.AddMessage(json.dumps(resp))


class DownloadFileTool(object):
    def __init__(self):
        self.label = "Download Debug Files"
        self.description = "Download Debug Files"
        self.canRunInBackground = True

    def getParameterInfo(self):
        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        submit_path = arcpy.Parameter(
            name="submit_path",
            displayName="Submit File",
            direction="Input",
            datatype="File",
            parameterType="Required")
        submit_path.value = os.path.join(os.path.dirname(__file__), "submit-res.json")

        err_out = arcpy.Parameter(
            name="err_out",
            displayName="File Type",
            direction="Input",
            datatype="String",
            parameterType="Required")
        err_out.value = "stderr"
        err_out.filter.type = "ValueList"
        err_out.filter.list = ["stderr", "stdout"]

        return [bm_path, submit_path, err_out]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[0].valueAsText
        submit_path = parameters[1].valueAsText
        err_out = parameters[2].valueAsText
        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            spark = bluemix["spark"]
            with open(submit_path) as submit_file:
                submit = json.load(submit_file)
                url = "https://spark.bluemix.net/tenant/data/workdir/" + submit["submissionId"] + "/" + err_out
                headers = {
                    "X-Spark-service-instance-id": spark["instance_id"]
                }
                r = requests.get(url,
                                 auth=(spark["tenant_id"], spark["tenant_secret"]),
                                 headers=headers
                                 )
                with open(os.path.join(os.path.dirname(__file__), err_out + ".txt"), "w") as open_file:
                    open_file.write(r.text)


class SubmitTool(object):
    def __init__(self):
        self.wait_try = 0
        self.wait_max = 30
        self.wait_sec = 7
        self.running = True
        self.spark = {}
        self.storage = {}
        self.label = "GeoBin Submit"
        self.description = "GeoBin Submit"
        self.canRunInBackground = True

    def getParameterInfo(self):
        out_fc = arcpy.Parameter(
            name="out_fc",
            displayName="out_fc",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")
        out_fc.symbology = os.path.join(os.path.dirname(__file__), "GeoBin.lyrx")

        bm_path = arcpy.Parameter(
            name="bm_path",
            displayName="Bluemix JSON Path",
            direction="Input",
            datatype="File",
            parameterType="Required")
        bm_path.value = os.path.join(os.path.dirname(__file__), "bluemix.json")

        input_path = arcpy.Parameter(
            name="input_path",
            displayName="Swift Input Path",
            direction="Input",
            datatype="String",
            parameterType="Required")
        input_path.value = "swift2d://trips.thunder/trips-1M.csv"

        output_path = arcpy.Parameter(
            name="output_path",
            displayName="Swift Output Path",
            direction="Input",
            datatype="String",
            parameterType="Required")
        output_path.value = "swift2d://output.thunder/GeoBins"

        bin_size = arcpy.Parameter(
            name="bin_size",
            displayName="Bin Size",
            direction="Input",
            datatype="String",
            parameterType="Required")
        bin_size.value = "0.001"

        return [out_fc, bm_path, input_path, output_path, bin_size]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        bm_path = parameters[1].valueAsText
        inp_path = parameters[2].valueAsText
        out_path = parameters[3].valueAsText
        bin_size = parameters[4].valueAsText

        with open(bm_path) as bm_file:
            bluemix = json.load(bm_file)
            epoch = int(time.time())
            spark = bluemix["spark"]
            storage = bluemix["storage"]
            url = "https://spark.bluemix.net/v1/submissions/create"
            headers = {"X-Requested-With": "spark-submit"}
            data = {
                "action": "CreateSubmissionRequest",
                "appArgs": [
                    "--primary-py-file",
                    "GeoBin.py"
                ],
                "appResource": "{}/GeoBin.py".format(spark["instance_id"]),
                "clientSparkVersion": "2.0",
                "mainClass": "org.apache.spark.deploy.PythonRunner",
                "sparkProperties": {
                    "spark.app.name": "GeoBin{}".format(epoch),
                    "spark.files": "{}/GeoBin.py".format(spark["instance_id"]),
                    "spark.service.spark_version": "2.0",
                    "spark.service.tenant_id": spark["tenant_id"],
                    "spark.service.instance_id": spark["instance_id"],
                    "spark.service.tenant_secret": spark["tenant_secret"],
                    "spark.service.user.fs.swift2d.impl": "com.ibm.stocator.fs.ObjectStoreFileSystem",
                    "spark.service.user.fs.swift2d.service.thunder.auth.method": "keystoneV3",
                    "spark.service.user.fs.swift2d.service.thunder.auth.endpoint.prefix": "endpoints",
                    "spark.service.user.fs.swift2d.service.thunder.auth.url": storage[
                                                                                  "auth_url"] + "/v3/auth/tokens",
                    "spark.service.user.fs.swift2d.service.thunder.region": storage["region"],
                    "spark.service.user.fs.swift2d.service.thunder.tenant": storage["projectId"],
                    "spark.service.user.fs.swift2d.service.thunder.username": storage["userId"],
                    "spark.service.user.fs.swift2d.service.thunder.password": storage["password"],
                    "spark.service.user.fs.swift2d.service.thunder.public": "true",
                    "spark.service.user.input.path": inp_path,
                    "spark.service.user.output.path": out_path,
                    "spark.service.user.cell.size": bin_size
                }
            }
            with open(os.path.join(os.path.dirname(__file__), "submit-req.json"), "w") as f:
                f.write(json.dumps(data))
            r = requests.post(url,
                              headers=headers,
                              json=data
                              )
            resp = r.json()
            text = json.dumps(resp)
            if "success" in resp and resp["success"]:
                arcpy.AddMessage(text)
                with open(os.path.join(os.path.dirname(__file__), "submit-res.json"), "w") as f:
                    f.write(text)
            else:
                arcpy.AddError(text)
