import re
import sys
import time
import json
import logging
import datetime
from os        import makedirs
from typing    import List, Pattern
from pprint    import pprint
from pathlib   import Path
from sqlite3   import connect, Connection
from itertools import count

import pandas as pd

DT_NOW    = datetime.datetime.now()
TIMESTAMP = f"{DT_NOW.year}-{DT_NOW.month}-{DT_NOW.day}-{DT_NOW.hour}-{DT_NOW.minute}-{DT_NOW.second}"

TRUE_STRINGS  = ["True", "T", "true", "t", "y", "Y", "yes"]
FALSE_STRINGS = ["False", "F", "false", "f", "n", "N", "no"]

def desacentuar(linha:str) -> str: 
    """ troca letras acentuadas por sua versao 'sem acento', incluindo c cedilha """
    if not hasattr(desacentuar, "patterns"):
        logging.debug("criando patterns para funcao desacentuar")
        desacentuar.patterns = [
             (re.compile(r'[áàâã]'), 'a'),
             (re.compile(r'[éê]'),   'e'),
             (re.compile(r'[í]'),    'i'),
             (re.compile(r'[óô]'),   'o'),
             (re.compile(r'[ú]'),    'u'),
             (re.compile(r'[ç]'),    'c'),
             (re.compile(r'\s+'),     ' ') ]
    linha = linha.lower()
    for pattern, repl in desacentuar.patterns:
        linha = pattern.sub(repl, linha).strip()
    return linha 

class Conf: 
    def __init__(self, filepath: Path):
        with open(filepath, "rt") as fhandle:
            self.conf_json = json.load(fhandle)
        self.file_path        = filepath
        self.txts_diretorio   = self.conf_json["txts_diretorio"]
        self.debug_log        = self.conf_json["debug_log"] in TRUE_STRINGS
        self.padroes_json     = self.conf_json["padroes_json"]
        self.diretorio_output = self.conf_json["diretorio_output"]

        self.create_output_dir()
        return

    def create_output_dir(self):
        makedirs(self.diretorio_output, exist_ok=True)
        return

    def __repr__(self):
        return f"Conf(Path('{self.file_path.name}'))" 

class Padrao: 
    def __init__(
        self,
        pattern_name: str,
        re_patterns: List[str],
        linhas_contexto: int
    ):
        self.pattern_name = pattern_name
        self.linhas_contexto = linhas_contexto
        self.patterns = self.make_patterns(re_patterns)
        return

    def make_patterns(self, patterns: List[str]):
        return [ re.compile(f"\W{desacentuar(regex)}\W", re.I) for regex in patterns ]

    def __repr__(self):
        return f"<Padrao pattern_name='{self.pattern_name}'>'"

class Ocorrencia: 
    __slots__ = ['lines', 'pattern_name', 'filename', 'linha_match']

    def __init__(
            self,
            lines: List[str],
            pattern_name: str,
            filename: str,
            linha_match: int
        ):
        self.lines = lines
        self.pattern_name = pattern_name
        self.filename = filename
        self.linha_match = linha_match
        return

    def __repr__(self):
        return f"<Ocorrencia pattern_name='{self.pattern_name}' filename='{self.filename}'>"

    def to_dict(self):
        return {
            "lines": '\n'.join(self.lines),
            "pattern_name": self.pattern_name,
            "filename": self.filename,
            "linha_match": self.linha_match
        }

class TxtFile: 
    def __init__(self, txtpath: Path):
        self.filename = txtpath.name
        with open(txtpath, "rt") as txthandle:
            self.lines = [ desacentuar(line) for line in txthandle.readlines() ]
        self.lines_len = len(self.lines)
        return

    def __repr__(self):
        return f"TxtFile(Path('{self.filename}'))"

    def make_ocorrencias_dataframe(self, padroes: List[Padrao]):
        ocorrencias = []
        for (i, line) in zip(count(), self.lines):
            for padrao in padroes:
                for pattern in padrao.patterns:
                    if pattern.search(line) != None:
                        min_index = max(0, i - padrao.linhas_contexto)
                        max_index = min(self.lines_len, i + padrao.linhas_contexto)
                        ocorrencias.append(Ocorrencia(
                            self.lines[min_index:max_index],
                            padrao.pattern_name,
                            self.filename,
                            i))
                        break
        return pd.DataFrame([ o.to_dict() for o in ocorrencias ])

    def write_ocorrencias(self, padroes: List[Padrao], db_conn: Connection, tablename: str) -> int:
        """retorn qtd de ocorrencias achadas"""
        ocorrencias = self.make_ocorrencias_dataframe(padroes)
        ocorrencias.to_sql(name=tablename, con=db_conn, if_exists='append', method='multi')
        return ocorrencias.count()['lines']

def gen_padroes(conf: Conf):
    padroes = []
    with open(conf.padroes_json, "rt") as padroes_handle:
        padroes_json = json.load(padroes_handle)
    for padrao_dict in padroes_json["padroes"]:
        padroes.append(Padrao(
            padrao_dict["pattern_name"],
            padrao_dict["re_patterns"],
            padrao_dict["linhas_contexto"]
            ))
    logging.info(f"{len(padroes)} padroes gerados.")
    return padroes

def setup_logging(is_debug: bool):
    if is_debug:
        print("debug log ON")
        log_filename = f"grepperbatch_{TIMESTAMP}.log"
        logging_format = '%(asctime)s %(levelname)s - %(message)s'
        log_handlers = [logging.StreamHandler(sys.stdout), logging.FileHandler(log_filename)]
        logging.basicConfig(level=logging.INFO, format=logging_format, handlers=log_handlers)
    else:
        logging_format = '%(asctime)s %(levelname)s - %(message)s'
        logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)], format=logging_format, level=logging.ERROR)
    return

def gen_conf_path_str():
    def print_file(filepath: str):
        with open(filepath, "rt") as fhandle:
            try:
                json_dict = json.load(fhandle)
                pprint(json_dict)
            except json.decoder.JSONDecodeError:
                print(f"{filepath} não é JSON válido!")
        return

    if len(sys.argv) > 1 and Path(sys.argv[1]).is_file():
        conf_path_str = sys.argv[1]
        print_file(conf_path_str)
        return conf_path_str
    is_good_conf = False
    is_confirm   = False
    while (not is_good_conf) or (not is_confirm):
        is_good_conf = False
        is_confirm   = False
        conf_path_str = input(f"Caminho para configuração: ")
        if Path(conf_path_str).is_file():
            is_good_conf = True
            print_file(conf_path_str)
            confirm       = input(f"Confirma configuração {conf_path_str} (y/n)? ")
            is_confirm = confirm in TRUE_STRINGS
        else:
            print("Arquivo não existe.")
    return conf_path_str

def gen_ocorrencias(conf: Conf):
    db_path     = Path(conf.diretorio_output).joinpath(f"ocorrencias_{TIMESTAMP}.db")
    db_conn     = connect(db_path.as_posix())
    txtpaths    = [ tp for tp in Path(conf.txts_diretorio).iterdir() if tp.suffixes == [".txt"] ]
    txtfiles    = [ TxtFile(txtpath) for txtpath in txtpaths ]
    padroes     = gen_padroes(conf)
    for txtfile in txtfiles:
        ocorrencias_count = txtfile.write_ocorrencias(padroes, db_conn, "ocorrencias")
        logging.info(f"{ocorrencias_count} ocorrencias em {txtfile.filename}.")
    logging.info(f"Ocorrencias de {len(txtfiles)} arquivos salvo em {db_path.name}.")
    return

def main():
    conf = Conf(Path(gen_conf_path_str()))
    setup_logging(conf.debug_log)
    gen_ocorrencias(conf)

if __name__ == "__main__":
    main()

