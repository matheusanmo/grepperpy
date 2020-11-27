from pathlib import Path
from functools import partial
import re
from copy import deepcopy
import time
import sys
import logging
import json
from typing import List, Pattern, TypeVar

T = TypeVar('T')

def flatten(xss: List[List[T]]) -> List[T]:
    return [x for xs in xss for x in xs]

def desacentuar(linha:str) -> str:
    """ troca letras acentuadas por sua versao 'sem acento', incluindo c cedilha """
    if not hasattr(desacentuar, "patterns"):
        logging.info("criando atributos para funcao desacentuar")
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

class TermoPesquisa:
    """ Recebe string de termo a ser buscado, normaliza e expoe pattern re """
    def __init__(self, termo: str):
        if len(termo) < 3:
            logging.warning(f"TermoPesquisa.__init__: descartando termo com menos 3 letras: '{termo}'")
            self.pattern    = termo
            self.descartado = True
            self.search     = lambda _ : None
            return
        termo_normalizado  = "\W"+ desacentuar(termo) + "\W"
        self.compiled = re.compile(termo_normalizado)
        self.pattern    = termo
        self.search   = self.compiled.search

class Caderno:
    """Carrega o .txt a partir do filename e expoe funcoes para gerar ocorrencias etc"""
    def __init__(self, txtpath: Path, context_lines=3):
        self.context_lines = context_lines
        self.txtpath = txtpath
        with open(txtpath, "rt") as filehandle:
            self.lines = filehandle.readlines()

    def ocorrencias(self, patterns: List[Pattern]):
        try:
            return self._ocorrencias
        except AttributeError:
            pass
        self._ocorrencias = []
        lines_len = len(self.lines)
        for i in range(lines_len):
            line_desacentuada = desacentuar(self.lines[i])
            for pattern in patterns:
                if pattern.search(line_desacentuada):
                    new_ocorrencia = dict()
                    context_begin  = 0         if i - self.context_lines < 0         else i - self.context_lines
                    context_end    = lines_len if i + self.context_lines > lines_len else i + self.context_lines
                    new_ocorrencia["filename"] = self.txtpath.name
                    new_ocorrencia["pattern"]  = pattern.pattern
                    new_ocorrencia["line_num"] = i - 1
                    new_ocorrencia["lines"]    = list(self.lines[context_begin:context_end])
                    self._ocorrencias.append(deepcopy(new_ocorrencia))
        logging.info(f"{len(self._ocorrencias)} ocorrencias em {self.txtpath}")
        return self._ocorrencias

def gen_txt_paths(txt_dir: str) -> List[dict]:
    dirpath   = Path(txt_dir)
    dir_files = list(dirpath.iterdir())
    txt_paths = filter(lambda p: p.suffix == ".txt", dir_files)
    return txt_paths

def dict_output(txtdir: str, d: dict):
    timestamp = int(time.time())
    json_filename = f"{txtdir}_{timestamp}.json"
    with open(json_filename, "wt") as json_handle:
        json.dump(d, json_handle, sort_keys=True, indent=4)
    return json_filename

def gen_confdict() -> dict:
    confpath = Path("./grepperbatch.conf.json")
    confhandle = open(confpath, "rt")
    return json.load(confhandle)

def ocorrencias_by_termo(ocorrencias: List[dict]) -> dict:
    filenames = set(map(lambda d: d["filename"], ocorrencias))
    files_dicts = dict()
    for filename in filenames:
        files_dicts[filename] = dict()
        for ocorrencia in ocorrencias:
            if ocorrencia["filename"] == filename:
                try:
                    files_dicts[filename][ocorrencia["pattern"]] += 1
                except KeyError:
                    files_dicts[filename][ocorrencia["pattern"]] = 1
    return files_dicts

def ocorrencias_by_filename(ocorrencias: List[dict]) -> dict:
    filenames = set(map(lambda d: d["filename"], ocorrencias))
    files_dicts = dict()
    for filename in filenames:
        files_dicts[filename] = dict()
        for ocorrencia in ocorrencias:
            if ocorrencia["filename"] == filename:
                try:
                    files_dicts[filename][ocorrencia["pattern"]] += 1
                except KeyError:
                    files_dicts[filename][ocorrencia["pattern"]] = 1
    return files_dicts

def ocorrencias_by_pattern(ocorrencias: List[dict]) -> dict:
    patterns = set(map(lambda d: d["pattern"], ocorrencias))
    patterns_dicts = dict()
    for pattern in patterns:
        patterns_dicts[pattern] = dict()
        for ocorrencia in ocorrencias:
            if ocorrencia["pattern"] == pattern:
                try:
                    patterns_dicts[pattern][ocorrencia["filename"]] += 1
                except KeyError:
                    patterns_dicts[pattern][ocorrencia["filename"]] = 1
    return patterns_dicts

def setup_logging(is_debug: bool):
    if is_debug:
        print("debug log ON")
        timestamp = int(time.time())
        log_filename = f"grepperbatch_{timestamp}.log"
        logging_format = '%(asctime)s %(levelname)s - %(message)s'
        log_handlers = [logging.StreamHandler(sys.stdout), logging.FileHandler(log_filename)]
        logging.basicConfig(level=logging.DEBUG, format=logging_format, handlers=log_handlers)
    else:
        logging_format = '%(asctime)s %(levelname)s - %(message)s'
        logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)], format=logging_format, level=logging.DEBUG)
    return

def gen_termospesquisa(txtpath: str) -> List[Pattern]:
    """ recebe o caminho de um arquivo de texto e retorna uma lista contendo 
        TermoPesquisa para cada linha valida"""
    with open(txtpath, 'rt') as txthandle:
        lines = txthandle.readlines()
    return list(map(TermoPesquisa, lines))

CONFDICT = gen_confdict()
def main():
    is_debug_on = True if CONFDICT["debug_log"] in ["1", "T", "True", "true"] else False
    setup_logging(is_debug_on)

    txts_paths = gen_txt_paths(CONFDICT["txts_diretorio"])
    termospesquisa   = gen_termospesquisa(CONFDICT["termos_caminho"])
    cadernos          = list(map(Caderno, txts_paths))
    logging.info(f"{len(termospesquisa)} patterns criados a partir de {CONFDICT['termos_caminho']}")
    logging.info(f"{len(cadernos)} Cadernos no diretorio {CONFDICT['txts_diretorio']}")
    ocorrencias       = flatten(list(map(lambda c: c.ocorrencias(termospesquisa), cadernos)))
    output_final      = {
        "ocorrencias_by_filename": ocorrencias_by_filename(ocorrencias),
        "ocorrencias_by_pattern":  ocorrencias_by_pattern(ocorrencias),
        "total_ocorrencias":       len(ocorrencias),
        "ocorrencias":             ocorrencias }
    output_filename = dict_output(CONFDICT["txts_diretorio"], output_final)

    logging.info(f"relatorio salvo como {output_filename}")
    logging.shutdown()
    return
    
if __name__ == "__main__":
    main()

