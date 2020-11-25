import re
import sys
import json
import logging
from typing    import List, Pattern
from pathlib   import Path
from itertools import count

OCORRENCIAS_CONTEXT_LINES = 3

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
        logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)], format=logging_format, level=logging.ERROR)
    return

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

class Padrao:
    def __init__(self,
            displayname:        str, 
            re_patterns:        List[str],
            linhas_alcance:     int,
            minimo_ocorrencias: int

        ):
        self.displayname        = displayname
        self.linhas_alcance     = linhas_alcance
        self.minimo_ocorrencias = minimo_ocorrencias
        self.gen_patterns(re_patterns)
        return

    def __repr__(self):
        return f"<Padrao displayname='{self.displayname}'>"

    def gen_patterns(self, patterns):
        self.re_patterns = [ re.compile(f"\W{desacentuar(patt)}\W", re.I) for patt in patterns ]
        return

    def to_dict(self):
        return

class Ocorrencia:
    __slots__ = ['lines', 'pattern_displayname', 'filename', 'linha_match', "_dict"]
    def __init__(self,
                 lines: List[str],
                 pattern_displayname: str,
                 filename: str,
                 linha_match: str):
        self.lines = lines
        self.pattern_displayname = pattern_displayname
        self.filename = filename
        self.linha_match = linha_match
        return

    def __repr__(self):
        return f"<Ocorrencia pattern_displayname='{self.pattern_displayname}' filename='{self.filename}'>"
        
    def to_dict(self):
        try:
            return self._dict
        except AttributeError:
            pass
        self._dict                        = dict()
        self._dict["lines"]               = self.lines
        self._dict["pattern_displayname"] = self.pattern_displayname
        self._dict["filename"]            = self.filename
        self._dict["linha_match"]         = self.linha_match
        return self.to_dict()
        

class Conf:
    def __init__(self, filepath: Path):
        with open(filepath, "rt") as fhandle:
            self.conf_json  = json.load(fhandle)
        self.txts_diretorio = self.conf_json["txts_diretorio"]
        self.debug_log      = self.conf_json["debug_log"]
        self.termos_caminho = self.conf_json["termos_caminho"]
        return

class TxtFile:
    def __init__(self, txtpath: Path):
        self.filename = txtpath.name
        with open(txtpath, "rt") as txthandle:
            self.lines = [ desacentuar(line) for line in txthandle.readlines() ]
        self.lines_len = len(self.lines)
        return

    def __repr__(self):
        return f"TxtFile('{self.filename}')"

    def make_ocorrencias(self, padroes: List[Padrao]):
        ocorrencias = []
        for (i, line) in zip(count(), self.lines):
            for padrao in padroes:
                for re_pattern in padrao.re_patterns:
                    if re_pattern.search(line) != None:
                        min_index = max(0, i - OCORRENCIAS_CONTEXT_LINES)
                        max_index = min(self.lines_len, i + OCORRENCIAS_CONTEXT_LINES)
                        ocorrencias.append(Ocorrencia(self.lines[min_index:max_index],
                            padrao.displayname,
                            self.filename,
                            str(i)))
                        break
        return ocorrencias

def gen_padroes(conf: Conf):
    padroes = []
    with open(conf.termos_caminho, "rt") as padroes_handle:
        padroes_json = json.load(padroes_handle)
    for padrao_dict in padroes_json["padroes"]:
        padroes.append(Padrao(
            padrao_dict["displayname"],
            padrao_dict["re_patterns"],
            padrao_dict["linhas_alcance"],
            padrao_dict["minimo_ocorrencias"]
            ))
    return padroes

def main():
    setup_logging(False)
    conf        = Conf(Path("./grepperbatch.conf.json"))
    txtpaths    = [ tp for tp in Path(conf.txts_diretorio).iterdir() if tp.suffixes == [".txt"] ]
    txtfiles    = [ TxtFile(txtpath) for txtpath in txtpaths ]
    padroes     = gen_padroes(conf)
    ocorrencias = txtfiles[0].make_ocorrencias(padroes)
    import ipdb; ipdb.set_trace()
    with open("./tmp.json", "wt") as fh:
        json.dump([o.to_dict() for o in ocorrencias], fh, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()

