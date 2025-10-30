from dataclasses import dataclass, field

@dataclass
class ChromadbConfig:
    path:str = field(default = 'chroma_db')
    collection:str = field(default = 'networking-platform')