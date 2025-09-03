# multi_pacs_query10_0_20_opt_workers.py
# Version 10.0.20-opt with MAX_WORKERS support for each server,
# modality/exclude filtering and proper CSV saving.

import csv
import argparse
import warnings
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pynetdicom import AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind
from pynetdicom.presentation import PresentationContext
from pydicom.dataset import Dataset
import pydicom

warnings.filterwarnings("ignore")  # suppress pydicom warnings


def load_servers(cfg_file):
    """Load servers from cfg file: ip port aet [max_workers]"""
    servers = []
    with open(cfg_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                parts = line.split()
                ip, port, aet = parts[:3]
                max_workers = int(parts[3]) if len(parts) > 3 else 4
                servers.append(
                    {"ip": ip, "port": int(port), "aet": aet, "max_workers": max_workers}
                )
    return servers


def query_server(ip, port, aet, start_datetime, end_datetime, local_aet):
    """C-FIND at STUDY level"""
    ae = AE(ae_title=local_aet)
    context = PresentationContext()
    context.abstract_syntax = StudyRootQueryRetrieveInformationModelFind
    context.transfer_syntax = [
        pydicom.uid.ExplicitVRLittleEndian,
        pydicom.uid.ImplicitVRLittleEndian,
    ]
    ae.requested_contexts = [context]

    ds = Dataset()
    ds.QueryRetrieveLevel = "STUDY"
    ds.StudyDate = start_datetime.strftime("%Y%m%d")
    ds.StudyInstanceUID = ""
    ds.AccessionNumber = ""
    ds.NumberOfStudyRelatedInstances = ""
    ds.NumberOfStudyRelatedSeries = ""
    ds.ModalitiesInStudy = ""
    ds.StudyTime = ""

    if start_datetime.time() != datetime.min.time() or end_datetime.time() != datetime.max.time():
        start_time_str = start_datetime.strftime("%H%M%S")
        end_time_str = end_datetime.strftime("%H%M%S")
        ds.StudyTime = f"{start_time_str}-{end_time_str}"

    assoc = ae.associate(ip, port, ae_title=aet)
    results = []
    if assoc.is_established:
        responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
        for (status, identifier) in responses:
            if status and identifier:
                study_uid = getattr(identifier, "StudyInstanceUID", None)
                study_img = getattr(identifier, "NumberOfStudyRelatedInstances", None)
                study_ser = getattr(identifier, "NumberOfStudyRelatedSeries", None)
                study_date = getattr(identifier, "StudyDate", None)
                accession = getattr(identifier, "AccessionNumber", None)
                modality_raw = getattr(identifier, "ModalitiesInStudy", None)

                try:
                    study_img = int(study_img) if study_img is not None else 0
                except Exception:
                    study_img = 0
                try:
                    study_ser = int(study_ser) if study_ser is not None else 0
                except Exception:
                    study_ser = 0

                if modality_raw:
                    if isinstance(modality_raw, pydicom.multival.MultiValue):
                        modalities_list = [str(m) for m in modality_raw]
                    elif isinstance(modality_raw, (list, tuple)):
                        modalities_list = [str(m) for m in modality_raw]
                    else:
                        modalities_list = [str(modality_raw)]
                else:
                    modalities_list = []

                results.append(
                    {
                        "StudyDate": study_date,
                        "StudyInstanceUID": study_uid,
                        "NumberOfStudyRelatedInstances": study_img,
                        "NumberOfStudyRelatedSeries": study_ser,
                        "AccessionNumber": accession,
                        "ModalityList": modalities_list,
                    }
                )
        assoc.release()
    return results


def query_study_series(ip, port, aet, study_uid, local_aet):
    """C-FIND at SERIES level"""
    ae = AE(ae_title=local_aet)
    context = PresentationContext()
    context.abstract_syntax = StudyRootQueryRetrieveInformationModelFind
    context.transfer_syntax = [
        pydicom.uid.ExplicitVRLittleEndian,
        pydicom.uid.ImplicitVRLittleEndian,
    ]
    ae.requested_contexts = [context]

    ds = Dataset()
    ds.QueryRetrieveLevel = "SERIES"
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = ""
    ds.Modality = ""

    assoc = ae.associate(ip, port, ae_title=aet)
    series_list = []
    if assoc.is_established:
        responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
        for (status, identifier) in responses:
            if status and identifier:
                series_uid = getattr(identifier, "SeriesInstanceUID", None)
                modality = getattr(identifier, "Modality", None)
                if series_uid and modality:
                    series_list.append((series_uid, modality))
        assoc.release()
    return series_list


def query_server_with_4h_blocks(ip, port, aet, date_obj, local_aet):
    """Split into 4-hour blocks if results count is 500"""
    day_start = datetime.combine(date_obj.date(), datetime.min.time())
    day_end = datetime.combine(date_obj.date(), datetime.max.time())

    partial_results = query_server(ip, port, aet, day_start, day_end, local_aet)
    if len(partial_results) < 500:
        return partial_results

    results = []
    for i in range(6):
        block_start = day_start + timedelta(hours=4 * i)
        block_end = block_start + timedelta(hours=3, minutes=59, seconds=59)
        if block_end > day_end:
            block_end = day_end
        block_results = query_server(ip, port, aet, block_start, block_end, local_aet)
        results.extend(block_results)
    return results


def modality_list_intersects(modality_list, check_list):
    return not set(m.upper() for m in modality_list).isdisjoint(
        set(m.upper() for m in check_list)
    )


def modality_list_excludes(modality_list, exclude_list):
    return modality_list_intersects(modality_list, exclude_list)


def filter_study(study_modalities, modality_include, modality_exclude):
    if modality_exclude and modality_exclude != ["NONE"]:
        if modality_list_excludes(study_modalities, modality_exclude):
            return False
    if modality_include in (None, [], ["NONE"], ["*"]):
        return True
    return modality_list_intersects(study_modalities, modality_include)


def process_server(server, current_date, args):
    """Retrieve studies and series from server"""
    studies = {}
    srv_results = query_server_with_4h_blocks(
        server["ip"], server["port"], server["aet"], current_date, args.aet
    )
    for study in srv_results:
        uid = study.get("StudyInstanceUID")
        if not uid:
            continue
        series_list = query_study_series(
            server["ip"], server["port"], server["aet"], uid, args.aet
        )
        studies[uid] = {
            "SeriesCount": study.get("NumberOfStudyRelatedSeries", 0),
            "ImagesCount": study.get("NumberOfStudyRelatedInstances", 0),
            "SeriesList": series_list,
            "StudyDate": study.get("StudyDate", current_date.strftime("%Y%m%d")),
            "AccessionNumber": study.get("AccessionNumber", ""),
            "Modalities": study.get("ModalityList", []),
            "SourceServerAET": server["aet"],
        }
    return server["aet"], studies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", required=True, help="YYYYMMDD")
    parser.add_argument("--end_date", required=True, help="YYYYMMDD")
    parser.add_argument("--modality", nargs="+", default=["NONE"], help="Filter modality")
    parser.add_argument("--exclude", nargs="+", default=["NONE"], help="Exclude modality")
    parser.add_argument("--cfg", default="pacs_servers.cfg")
    parser.add_argument("--output", default=None)
    parser.add_argument("--aet", default="MY_AET")
    args = parser.parse_args()

    if not args.output:
        modality_str = "-".join(args.modality) if args.modality != ["NONE"] else "ALL"
        exclude_str = (
            "_exclude_" + "-".join(args.exclude)
            if args.exclude and args.exclude != ["NONE"]
            else ""
        )
        args.output = f"multi_pacs17_{modality_str}{exclude_str}_{args.start_date}_{args.end_date}.csv"

    servers = load_servers(args.cfg)
    target_server = servers[0]
    other_servers = servers[1:]

    current_date = datetime.strptime(args.start_date, "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d")

    while current_date <= end_date:
        print(f"Processing: {current_date.strftime('%Y%m%d')}")

        # target server
        _, target_studies = process_server(target_server, current_date, args)

        # parallel retrieval from other servers
        other_studies = {}
        futures = []
        for srv in other_servers:
            executor = ThreadPoolExecutor(max_workers=srv["max_workers"])
            futures.append(executor.submit(process_server, srv, current_date, args))
        for f in as_completed(futures):
            aet, studies = f.result()
            for uid, data in studies.items():
                other_studies.setdefault(uid, []).append(data)

        all_uids = set(target_studies.keys()) | set(other_studies.keys())
        filtered = set()
        for uid in all_uids:
            modalities = []
            if uid in target_studies:
                modalities += target_studies[uid]["Modalities"]
            if uid in other_studies:
                for entry in other_studies[uid]:
                    modalities += entry["Modalities"]
            modalities = list(set(m.upper() for m in modalities))
            if filter_study(modalities, args.modality, args.exclude):
                filtered.add(uid)

        missing_series_map = {}
        for uid in filtered:
            tgt_series = set(
                s[0] for s in target_studies.get(uid, {}).get("SeriesList", [])
            )
            if uid in other_studies:
                for entry in other_studies[uid]:
                    for s_uid, s_mod in entry["SeriesList"]:
                        if s_uid not in tgt_series:
                            if (args.exclude == ["NONE"] or s_mod.upper() not in [e.upper() for e in args.exclude]) and (
                                args.modality == ["NONE"]
                                or s_mod.upper() in [m.upper() for m in args.modality]
                            ):
                                missing_series_map.setdefault(uid, []).append(
                                    (s_uid, s_mod, entry["SourceServerAET"])
                                )

        with open(args.output, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if csvfile.tell() == 0:
                writer.writerow(
                    [
                        "StudyDate",
                        "StudyInstanceUID",
                        "AccessionNumber",
                        "SeriesCount",
                        "ImagesCount",
                        "Modality",
                        "SourceServerAET",
                        "MissingSeries",
                    ]
                )
                
            written_count = 0

            for uid in filtered:
                if uid in target_studies:
                    st = target_studies[uid]
                    mods = ",".join(sorted(set(m.upper() for m in st["Modalities"])))
                    miss = missing_series_map.get(uid, [])
                    if miss:
                        by_srv = {}
                        for s_uid, s_mod, s_aet in miss:
                            by_srv.setdefault(s_aet, []).append(f"{s_uid}({s_mod})")
                        for s_aet, misslist in by_srv.items():
                            writer.writerow(
                                [
                                    st["StudyDate"],
                                    uid,
                                    st["AccessionNumber"],
                                    st["SeriesCount"],
                                    st["ImagesCount"],
                                    mods,
                                    s_aet,
                                    ", ".join(misslist),
                                ]
                            )
                            written_count += 1
                    else:
                        writer.writerow(
                            [
                                st["StudyDate"],
                                uid,
                                st["AccessionNumber"],
                                st["SeriesCount"],
                                st["ImagesCount"],
                                mods,
                                st["SourceServerAET"],
                                "",
                            ]
                        )
                        written_count += 1
                elif uid in other_studies:
                    all_mods = set()
                    for entry in other_studies[uid]:
                        all_mods.update(m.upper() for m in entry["Modalities"])
                    mods = ",".join(sorted(all_mods))
                    miss = missing_series_map.get(uid, [])
                    by_srv = {}
                    for s_uid, s_mod, s_aet in miss:
                        by_srv.setdefault(s_aet, []).append(f"{s_uid}({s_mod})")
                    for s_aet, misslist in by_srv.items():
                        writer.writerow(
                            [
                                current_date.strftime("%Y%m%d"),
                                uid,
                                "",
                                0,
                                0,
                                mods,
                                s_aet,
                                ", ".join(misslist),
                            ]
                        )
                        written_count += 1

        print(f"Saved {written_count} records for date {current_date.strftime('%Y%m%d')}")
        current_date += timedelta(days=1)


if __name__ == "__main__":
    main()
