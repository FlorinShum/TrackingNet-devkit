import os
from os import path as osp
from tqdm import tqdm
import zipfile
import argparse
import shutil
from concurrent import futures

import pdb

def unzip_file(cur_dir, zip_files, remove=True):
	err_files = []
	for zip_file in zip_files:
		file_name = os.path.splitext(zip_file)[0]
		zip_file = osp.join(cur_dir, 'zips', zip_file)
		if not osp.exists(zip_file):
			raise FileNotFoundError
		frame_folder = osp.join(cur_dir, 'frames', file_name)
		extract = False
		try:
			with zipfile.ZipFile(zip_file) as zip_handler:
				if osp.exists(frame_folder):
					if not len(zip_handler.infolist()) == len(os.listdir(frame_folder)):
						shutil.rmtree(frame_folder)
						os.makedirs(frame_folder)
						extract = True
					else:
						same_number_files = True
				else:
					os.makedirs(frame_folder)
					extract = True
				if extract:
					zip_handler.extractall(os.path.join(frame_folder))
					same_number_files = len(zip_handler.infolist()) == len(os.listdir(frame_folder))
				if (not same_number_files):
					print("Warning:", frame_folder, "was not well extracted")
			if remove:
				os.remove(zip_file)
		except:
			# print(f'unzip {zip_file} failed, try another way.')
			err_files.append(zip_file)
			# os.system(f'unzip {zip_file} -d frame_folder -n -t ')
	return err_files


def split_list(file_list, num_seg=20):
	if len(file_list) < num_seg: return [file_list]
	seged_list = []
	seg_len = len(file_list) // num_seg
	start, end = 0, seg_len
	for _ in range(num_seg + 1):
		part = file_list[start:end]
		start += seg_len 
		end += seg_len 
		if len(part) > 0:
			seged_list.append(part)
	return seged_list

def main(trackingnet_dir="TrackingNet", overwrite_frames=False, chunks=[], threads=20):
	for chunk_folder in chunks:
		chunk_folder = chunk_folder.upper()
		zip_folder = os.path.join(trackingnet_dir, chunk_folder, "zips")
		file_list = [name for name in os.listdir(zip_folder) if '.zip' in name]
		splited_file_list = split_list(file_list, threads)
		cur_dir = osp.join(trackingnet_dir, chunk_folder)
		print(chunk_folder)
		
		# if chunk_folder == 'TRAIN_5':
		# 	pdb.set_trace()

		# with futures.ProcessPoolExecutor(max_workers=threads) as executor:
		# 	print('start submit')
		# 	fs = [executor.submit(unzip_file, cur_dir, part_list, True) for part_list in splited_file_list]
		# 	for f in fs:
		# 		[print(err) for err in f.result()]
		
		for part_list in splited_file_list:
			err_files = unzip_file(cur_dir, part_list, True)
		[print(err) for err in err_files]
						

if __name__ == "__main__": 
	p = argparse.ArgumentParser(description='Extract the frames for TrackingNet')
	p.add_argument('--trackingnet_dir', type=str, default='TrackingNet',
		help='Main TrackingNet folder.')
	p.add_argument('--overwrite_frames', action='store_true',
		help='Folder where to store the frames.')
	p.add_argument('--chunk', type=str, default="ALL",
		help='List of chunks to elaborate [ALL / Train / Test / 4 / 1,2,5].')
	p.add_argument('--threads', type=int, default=20,
		help='number of threads to unzip files')	
	args = p.parse_args()

	try:		
		if ("ALL" in args.chunk.upper()):
			chunk = ["TRAIN_"+str(c) for c in range(12)]
			chunk.insert(0, "TEST")		
		elif ("TEST" in args.chunk.upper()):
			chunk = ["TEST"]
		elif ("TRAIN" in args.chunk.upper()):
			chunk = ["TRAIN_"+str(c) for c in range(12)]
		else :
			chunk = ["TRAIN_"+str(int(c)) for c in args.chunk.split(",")]		
	except:		
		chunk = []


	print("extracting the frames for the following chunks:")
	print(chunk)
	# python concurrent_extract.py --trackingnet_dir /hy-tmp/train_data/trackingnet  --chunk TRAIN --threads 64
	# python concurrent_extract.py --trackingnet_dir /hy-tmp/tn_all  --chunk TEST --threads 20
	main(args.trackingnet_dir, args.overwrite_frames, chunk, args.threads)


