import tempfile
from .config import HPCHelperConfig, PreserveFormatDict


class Gadopt_HPC_Script:
    def __init__(self, cfg: HPCHelperConfig, executor: str, cmd: list[str]):
        self.script_file = tempfile.NamedTemporaryFile()
        with open(self.script_file.name, "w") as f:
            f.write(
                cfg.template.format_map(
                    PreserveFormatDict(
                        header=cfg.header, prescript=cfg.prescript, executor=executor, command=" ".join(cmd)
                    )
                )
            )

    def __enter__(self) -> str:
        return self.script_file.name

    def __exit__(self, type, value, traceback):
        self.script_file.close()
