#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json,logging,os,queue,signal,sys,threading,time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable,Optional,Dict,Any
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    HAVE_WATCHDOG=True
except Exception:
    HAVE_WATCHDOG=False
TEMP_SUFFIXES={".tmp",".partial",".part",".crdownload"}
TEMP_PREFIXES={".~","~$","."}
CSV_EXTENSIONS={".csv"}
def is_probably_temp(path:Path)->bool:
    name=path.name
    if any(name.startswith(p) for p in TEMP_PREFIXES):return True
    if any(name.endswith(suf) for suf in TEMP_SUFFIXES):return True
    return False
def is_csv(path:Path)->bool:
    return path.suffix.lower() in CSV_EXTENSIONS
@dataclass
class Config:
    watch_dir:Path;out_dir:Path;recursive:bool;process_existing:bool;json_lines:bool;overwrite:bool;indent:Optional[int];delimiter:Optional[str];quotechar:Optional[str];encoding:str;debounce_sec:float=1.25;poll_interval:float=2.0
class Debouncer:
    def __init__(self,delay_sec:float,action):
        self.delay=delay_sec;self.action=action;self._timers:Dict[Path,threading.Timer]={};self._lock=threading.Lock()
    def trigger(self,path:Path):
        with self._lock:
            t=self._timers.get(path)
            if t is not None:t.cancel()
            timer=threading.Timer(self.delay,self._run,args=(path,));self._timers[path]=timer;timer.daemon=True;timer.start()
    def _run(self,path:Path):
        with self._lock:self._timers.pop(path,None)
        self.action(path)
class Converter:
    def __init__(self,cfg:Config):
        self.cfg=cfg;self._work_q:"queue.Queue[Path]"=queue.Queue();self._stop=threading.Event();self._worker=threading.Thread(target=self._worker_main,daemon=True);self._worker.start()
    def enqueue(self,path:Path):
        if not is_csv(path)or is_probably_temp(path)or not path.exists():return
        self._work_q.put(path)
    def stop(self):
        self._stop.set();self._work_q.put(None);self._worker.join(timeout=5)
    def _worker_main(self):
        while not self._stop.is_set():
            try:item=self._work_q.get(timeout=0.5)
            except queue.Empty:continue
            if item is None:break
            path:Path=item
            try:
                if not self._wait_stable(path,window=0.6,checks=3):
                    logging.warning("File never stabilized; skipping: %s",path);continue
                self._convert_file(path)
            except Exception as e:logging.exception("Failed to convert %s: %s",path,e)
    def _wait_stable(self,path:Path,window:float,checks:int)->bool:
        try:last=path.stat().st_size
        except FileNotFoundError:return False
        stable=0
        for _ in range(checks*2):
            time.sleep(window)
            try:size=path.stat().st_size
            except FileNotFoundError:return False
            if size==last:
                stable+=1
                if stable>=checks:return True
            else:stable=0;last=size
        return False
    def _unique_out_path(self,base:Path)->Path:
        if self.cfg.overwrite or not base.exists():return base
        i=1
        while True:
            candidate=base.with_name(f"{base.stem}_{i}{base.suffix}")
            if not candidate.exists():return candidate
            i+=1
    def _convert_file(self,csv_path:Path):
        rel=csv_path.relative_to(self.cfg.watch_dir) if hasattr(csv_path,"is_relative_to") and csv_path.is_relative_to(self.cfg.watch_dir) else csv_path.name
        logging.info("Converting %s",rel)
        out_name=csv_path.with_suffix(".jsonl" if self.cfg.json_lines else ".json").name
        out_path=self.cfg.out_dir/out_name
        out_path=self._unique_out_path(out_path)
        out_path.parent.mkdir(parents=True,exist_ok=True)
        tmp_path=out_path.with_suffix(out_path.suffix+".tmp")
        with open(csv_path,"r",encoding=self.cfg.encoding,newline="") as f:
            sample=f.read(4096);f.seek(0);dialect=None
            if not self.cfg.delimiter:
                try:dialect=csv.Sniffer().sniff(sample)
                except Exception:pass
            # Set delimiter and quotechar with proper fallbacks
            if self.cfg.delimiter:
                delimiter = self.cfg.delimiter
            elif dialect:
                delimiter = getattr(dialect, "delimiter", ",")
            else:
                delimiter = ","
            
            if self.cfg.quotechar:
                quotechar = self.cfg.quotechar
            elif dialect:
                quotechar = getattr(dialect, "quotechar", '"')
            else:
                quotechar = '"'
            
            reader_kwargs = {"delimiter": delimiter, "quotechar": quotechar}
            try:
                dict_reader=csv.DictReader(f,**reader_kwargs);fieldnames=dict_reader.fieldnames
                if not fieldnames or any(h is None or str(h).strip()=="" for h in fieldnames):raise ValueError("No valid header row detected")
                rows_iter:Iterable[Dict[str,Any]]=({k:v for k,v in row.items()} for row in dict_reader)
                self._write_json(rows_iter,tmp_path)
            except Exception:
                f.seek(0);rdr=csv.reader(f,**reader_kwargs)
                rows_iter2:Iterable[Dict[str,Any]]=({f"col{idx+1}":val for idx,val in enumerate(row)} for row in rdr)
                self._write_json(rows_iter2,tmp_path)
        os.replace(tmp_path,out_path);logging.info("Wrote %s",out_path)
    def _write_json(self,rows:Iterable[Dict[str,Any]],tmp_path:Path):
        if self.cfg.json_lines:
            with open(tmp_path,"w",encoding="utf-8") as out:
                for row in rows:out.write(json.dumps(row,ensure_ascii=False));out.write("\n")
        else:
            with open(tmp_path,"w",encoding="utf-8") as out:
                out.write("[");first=True
                for row in rows:
                    if not first:out.write(",")
                    else:first=False
                    obj=json.dumps(row,ensure_ascii=False,indent=self.cfg.indent)
                    out.write(obj)
                out.write("]")
if HAVE_WATCHDOG:
    class _WatchdogHandler(FileSystemEventHandler):
        def __init__(self,cfg:Config,conv:Converter,debouncer:Debouncer):
            super().__init__();self.cfg=cfg;self.conv=conv;self.debouncer=debouncer
        def on_created(self,event):self._maybe_handle(event)
        def on_modified(self,event):self._maybe_handle(event)
        def _maybe_handle(self,event):
            try:
                if getattr(event,"is_directory",False):return
                path=Path(event.src_path)
                if not is_csv(path):return
                self.debouncer.trigger(path)
            except Exception:logging.exception("Event handling failed")
class _PollingWatcher:
    def __init__(self,cfg:Config,conv:Converter):
        self.cfg=cfg;self.conv=conv;self._seen:Dict[Path,float]={};self._stop=threading.Event();self._thread=threading.Thread(target=self._run,daemon=True)
    def start(self):self._thread.start()
    def stop(self):self._stop.set();self._thread.join(timeout=5)
    def _run(self):
        logging.warning("watchdog not installed; using polling every %.1fs",self.cfg.poll_interval)
        while not self._stop.is_set():
            for p in self._scan_csvs():
                try:mtime=p.stat().st_mtime
                except FileNotFoundError:continue
                last=self._seen.get(p)
                if last is None or mtime>last:
                    self._seen[p]=mtime;self.conv.enqueue(p)
            time.sleep(self.cfg.poll_interval)
    def _scan_csvs(self)->Iterable[Path]:
        root=self.cfg.watch_dir
        it=root.rglob("*.csv") if self.cfg.recursive else root.glob("*.csv")
        for p in it:
            if not is_probably_temp(p):yield p
def parse_args(argv:Optional[Iterable[str]]=None)->Config:
    ap=argparse.ArgumentParser(description="Watch a directory for CSV files and convert them to JSON.")
    ap.add_argument("--watch",required=True,help="Directory to watch for CSV files")
    ap.add_argument("--out",default=None,help="Directory to write JSON files (default: same as --watch)")
    ap.add_argument("--recursive",action="store_true",help="Recurse into subdirectories")
    ap.add_argument("--process-existing",action="store_true",help="Process any existing CSV files on startup")
    ap.add_argument("--jsonl",action="store_true",help="Write JSON Lines (.jsonl) instead of an array (.json)")
    ap.add_argument("--overwrite",action="store_true",help="Overwrite existing outputs instead of uniquifying names")
    ap.add_argument("--indent",type=int,default=None,help="Indent level for pretty JSON (array mode only)")
    ap.add_argument("--delimiter",default=None,help="CSV delimiter (default: auto-sniff, then ',')")
    ap.add_argument("--quotechar",default=None,help="CSV quote character (default: auto-sniff, then double-quote)")
    ap.add_argument("--encoding",default="utf-8-sig",help="Input file encoding (default: utf-8-sig)")
    ap.add_argument("--log",default="INFO",help="Log level (DEBUG, INFO, WARNING, ERROR)")
    ns=ap.parse_args(argv)
    watch_dir=Path(ns.watch).expanduser().resolve()
    out_dir=Path(ns.out).expanduser().resolve() if ns.out else watch_dir
    if not watch_dir.exists() or not watch_dir.is_dir():ap.error(f"--watch path is not a directory: {watch_dir}")
    logging.basicConfig(level=getattr(logging,str(ns.log).upper(),logging.INFO),format="%(asctime)s %(levelname)s %(message)s",datefmt="%H:%M:%S")
    return Config(watch_dir=watch_dir,out_dir=out_dir,recursive=bool(ns.recursive),process_existing=bool(ns.process_existing),json_lines=bool(ns.jsonl),overwrite=bool(ns.overwrite),indent=ns.indent,delimiter=ns.delimiter,quotechar=ns.quotechar,encoding=ns.encoding)
def process_existing(cfg:Config,conv:Converter):
    def emit(p:Path):
        if p.is_file() and is_csv(p) and not is_probably_temp(p):conv.enqueue(p)
    if cfg.recursive:
        for p in cfg.watch_dir.rglob("*.csv"):emit(p)
    else:
        for p in cfg.watch_dir.glob("*.csv"):emit(p)
def main(argv:Optional[Iterable[str]]=None)->int:
    cfg=parse_args(argv);conv=Converter(cfg)
    def _shutdown(signum=None,frame=None):
        logging.info("Shutting down…")
        try:
            if HAVE_WATCHDOG and 'observer' in locals() and observer is not None:observer.stop();observer.join(timeout=5)
        except Exception:pass
        try:
            if 'poller' in locals() and poller is not None:poller.stop()
        except Exception:pass
        conv.stop();sys.exit(0)
    signal.signal(signal.SIGINT,_shutdown)
    if hasattr(signal,'SIGTERM'):signal.signal(signal.SIGTERM,_shutdown)
    if cfg.process_existing:process_existing(cfg,conv)
    if HAVE_WATCHDOG:
        debouncer=Debouncer(cfg.debounce_sec,conv.enqueue);handler=_WatchdogHandler(cfg,conv,debouncer);observer=Observer();observer.schedule(handler,str(cfg.watch_dir),recursive=cfg.recursive);observer.start();logging.info("Watching %s %s (watchdog)",cfg.watch_dir,"recursively" if cfg.recursive else "")
        try:
            while True:time.sleep(1)
        finally:observer.stop();observer.join(timeout=5)
    else:
        poller=_PollingWatcher(cfg,conv);poller.start();logging.info("Watching %s %s (polling)",cfg.watch_dir,"recursively" if cfg.recursive else "")
        try:
            while True:time.sleep(1)
        finally:poller.stop()
    return 0
if __name__=="__main__":raise SystemExit(main())
