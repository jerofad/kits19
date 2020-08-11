from pathlib import Path
from shutil import move
import os
import sys
import time

from tqdm import tqdm
import requests
import numpy as np

imaging_url = "https://kits19.sfo2.digitaloceanspaces.com/"
imaging_name_tmplt = "master_{:05d}.nii.gz"
temp_f = Path(__file__).parent / "temp.tmp"


def get_destination(i):
    destination = Path("__file__").parent.parent /\
        "data" / "case_{:05d}".format(i) / "imaging.nii.gz"
    if not destination.parent.exists():
        destination.parent.mkdir()
    return destination


def cleanup(bar, msg):
    bar.close()
    if temp_f.exists():
        temp_f.unlink()
    sys.exit()

from pySmartDL import SmartDL
def download_file_smart(uri, destination):
    obj = SmartDL(uri, destination) 
    obj.start()
    path = obj.get_dest()
    return path

def download_file(uri, chnksz, destination, expected_file_size):
    try:
        # The request handler should be opened in a with statement as mentioned here: https://2.python-requests.org/en/master/user/advanced/#body-content-workflow
        with requests.get(uri, stream=True) as response:
            with temp_f.open("wb") as f:
                bar = tqdm(
                    unit="KB", 
                    desc="case_{:05d}".format(cid), 
                    total=int(
                        np.ceil(int(response.headers["content-length"])/chnksz)
                    )
                )

                # If you have a good connection you can try to pass None to the chunk_size parameter: Here is the documentation https://2.python-requests.org/en/master/api/#requests.Response.iter_content
                for pkg in response.iter_content(chunk_size=chnksz):
                    if pkg == None:
                        print("End of stream")
                    f.write(pkg)
                    f.flush() #https://stackoverflow.com/questions/7127075/what-exactly-is-pythons-file-flush-doing
                    bar.update(int(len(pkg)/chnksz))

                bar.close()
    except KeyboardInterrupt:
        cleanup(bar, "KeyboardInterrupt")
    except Exception as e:
        print(e)
        cleanup(bar, str(e))

    
    dest_size = os.path.getsize(temp_f)
    if dest_size == expected_file_size:
        move(str(temp_f), str(destination))
        return True
    else:
        return False


if __name__ == "__main__":
    left_to_download = []
    for i in range(300):
        # if not get_destination(i).exists():
        #     left_to_download = left_to_download + [i]
        left_to_download = left_to_download + [i]


    print("{} cases to download...".format(len(left_to_download)))
    for i, cid in enumerate(left_to_download):
        print("Download {}/{}: ".format(
            i+1, len(left_to_download)
        ))
        destination = get_destination(cid)
        remote_name = imaging_name_tmplt.format(cid)
        uri = imaging_url + remote_name

        chnksz = 1000
        tries = 0
        skip = False

        expected_file_size = 0
        dest = get_destination(i)

        while True:
            try:
                tries = tries + 1

                with requests.get(uri, stream=True) as response:
                    expected_file_size = int(response.headers["content-length"])

                    # if size of file does not match the received content length then we download again other wise skip
                    if dest.exists():
                        dest_size = os.path.getsize(dest)
                        if dest_size == expected_file_size:
                            skip = True

                break
            except Exception as e:
                print("Failed to establish connection with server:\n")
                print(str(e) + "\n")
                if tries < 1000:
                    print("Retrying in 30s")
                    time.sleep(30)
                else:
                    print("Max retries exceeded")
                    sys.exit()

        if skip: 
            continue
        
        use_smart_download = False
        if use_smart_download:
            try:
                # TODO add a while loop here which can try to download the case until its complete
                print("Downloading file from: ", uri)
                download_file_smart(uri, str(destination))
            except KeyboardInterrupt:
                print("User stopped the download")
            except Exception as e:
                print(e)
        else:
            # TODO add a while loop here which can try to download the case until its complete
            print("Downloading file from: ", uri)

            download_complete = False
            while not download_complete:
                download_complete = download_file(uri, chnksz, destination, expected_file_size)
                




