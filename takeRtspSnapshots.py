import os
import datetime
import pymysql
import subprocess
import concurrent.futures
import dbconfig  # .py file in project

error_logs = []
serverPath = "SAMPLE_ABSOLUTE_PATH"
setMaxWorkers = 5


def getCamerasList():
    try:
        connectDB = pymysql.connect(
            host=dbconfig.DB_HOST,
            user=dbconfig.DB_USER,
            password=dbconfig.DB_PASSWORD,
            database=dbconfig.DB_NAME,
            charset=dbconfig.DB_CHARSET,
            autocommit=True,
        )
        with connectDB.cursor() as cur:
            cur.execute(
                "SELECT `zsk_db`, `name_camera`, `rtsp_link` FROM `cameras` WHERE `active` = 1"
            )
            rows = cur.fetchall()
        connectDB.close()
        if rows:
            return rows
        else:
            print("[INFO] No active cameras found in DB.")
            return None
    except Exception as e:
        error_logs.append(f"DB ERROR: {e}\n")
        return None


def sanitize_name(name):
    return name.lower().replace(" ", "_").replace("(", "-").replace(")", "-").replace(".", "+")


def rtspSnapshot(dbName, cameraName, rtspStream):
    cameraNameSan = sanitize_name(cameraName)
    filename = cameraNameSan + ".webp"
    retries = 0
    filesize = 0
    directory = f"{serverPath}/img/cameras_preview/{dbName}/"
    while retries < 3 and filesize == 0:
        try:
            print(f"\n---\nDATABASE: {dbName}\nCAMERA_NAME: {cameraNameSan}\nRetries: {retries}")
            os.makedirs(directory, mode=0o777, exist_ok=True)
            file_path = os.path.join(directory, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"{filename} --- removed old file")
            cmd = [
                "ffmpeg",
                "-y",
                "-rtsp_transport",
                "tcp",
                "-i",
                rtspStream,
                "-vframes",
                "1",
                "-r",
                "3",
                file_path,
                "-nostats",
                "-hide_banner",
                "-loglevel",
                "error",
            ]
            subprocess.run(cmd, check=True)
            filesize = os.path.getsize(file_path)
            print(f"{filename} --- saved filesize: {filesize}")
        except Exception as e:
            error_msg = f"ERROR processing {cameraNameSan} ({dbName}): {e}\n"
            error_logs.append(error_msg)
        retries += 1


def main():
    print(f"[START] {datetime.datetime.now()}")
    devices = []
    getDevices = getCamerasList()
    if getDevices is not None:
        for toAdd in getDevices:
            devices.append(
                {
                    "dbName": str(toAdd[0]),
                    "cameraName": str(toAdd[1]),
                    "rtspStream": str(toAdd[2]),
                }
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=setMaxWorkers) as executor:
        futures = [
            executor.submit(rtspSnapshot, d["dbName"], d["cameraName"], d["rtspStream"])
            for d in devices
        ]
        for future in concurrent.futures.as_completed(futures):
            pass

    print(f"[END] {datetime.datetime.now()}")

    if error_logs:
        nowDate = datetime.datetime.now().strftime("%d-%m-%Y")
        log_path = f"{serverPath}/logs/scripts/takeRtspSnapshots/{nowDate}.txt"
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a") as file:
            file.writelines(error_logs)


if __name__ == "__main__":
    main()
