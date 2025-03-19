import sys
from json import dumps as json_encode
from shlex import split as shlex_split
from inspect import signature
from time import perf_counter
from pathlib import Path
from functools import wraps
from prompt_toolkit import prompt
from prompt_toolkit.completion import NestedCompleter
from prompt_toolkit.shortcuts import yes_no_dialog
from prompt_toolkit.styles import Style
from rich.console import Console
from src.log.stash import Logstash

class CLI:
    """A Command-Line Interface (CLI) for log management."""

    def __init__(self):
        self.logstash = Logstash()
        self.console = Console()
        self.directory = Path(__file__).resolve().parent
        self.style = Style.from_dict({
            'dialog': 'bg:#000000',
            'dialog frame.label': 'bg:#ffffff #000000',
            'dialog.body': 'bg:#000000 #ffffff',
            'dialog shadow': 'bg:#000000',
        })

    def log(self, file_name: str = None, option: str = None, filter_file: str = None):
        """Handles log operations such as fetching info and filtering logs."""
        if not file_name:
            self.console.print("[red]noiseless: no log file provided to analyze.[/red]")
            return

        log_path = self.directory / "../../data/logs" / file_name
        if not log_path.exists():
            self.console.print(f"[red]noiseless: log file '{file_name}' not found.[/red]")
            return

        if option == "--info":
            self.console.print(f"Fetching info for [bold yellow]{file_name}[/bold yellow].", end=" ")
            start_time = perf_counter()
            info = self.logstash.get_info(log_path)
            elapsed_time = perf_counter() - start_time
            self.console.print(f"OK. Elapsed [bold yellow]{elapsed_time:.4f} seconds[/bold yellow].")
            self.console.print_json(json_encode(info))
            return

        if option == "--filter":
            if not filter_file:
                self.console.print("[red]noiseless: no filter file provided.[/red]")
                return
            
            filter_path = self.directory / "../../data/filters" / filter_file
            if not filter_path.exists():
                self.console.print(f"[red]noiseless: filter file '{filter_file}' not found.[/red]")
                return

            log_folder = self.directory / "../../data/filtered_logs" / log_path.stem
            if log_folder.exists() and not yes_no_dialog(
                title="Folder Exists",
                text=f"The folder '{log_folder}' already exists. Overwrite its contents?",
                style=self.style
            ).run():
                self.console.print("[yellow]noiseless: operation canceled.[/yellow]")
                return

            self.console.print(f"Filtering [bold yellow]{file_name}[/bold yellow]...", end=" ")
            start_time = perf_counter()
            filter_result = self.logstash.filter(log_path, filter_path, overwrite=True)
            elapsed_time = perf_counter() - start_time
            self.console.print(f"OK. Elapsed [bold yellow]{elapsed_time:.4f} seconds[/bold yellow].")
            self.console.print_json(json_encode(filter_result))

    def exit(self):
        """Exits the CLI."""
        self.console.print("[cyan]Goodbye![/cyan]")
        sys.exit(0)

    def clear(self):
        """Clears the console screen."""
        self.console.clear()

    def help(self):
        """Displays help text from a file."""
        help_file = self.directory / "resources/help.txt"
        if help_file.exists():
            self.console.print(help_file.read_text())
        else:
            self.console.print("[red]noiseless: Help file not found.[/red]")

    def draw_intro(self):
        """Displays the intro message."""
        intro_file = self.directory / "resources/intro_message.txt"
        if intro_file.exists():
            self.console.print(intro_file.read_text())

    def version(self, verbose: str = None):
        """Displays the CLI version."""
        version_file = self.directory / "resources/version.txt"
        if not version_file.exists():
            self.console.print("[red]noiseless: Version file not found.[/red]")
            return

        version_full = version_file.read_text()
        version_stripped = version_full.split("\n")[0]

        if verbose in ("-v", "--verbose"):
            self.console.print(version_full)
        else:
            cli_position = version_stripped.find("(cli)")
            self.console.print(version_stripped[:cli_position] if cli_position != -1 else version_stripped)

    def _run(self):
        """Starts the CLI event loop."""
        self.clear()
        self.draw_intro()

        try:
            while True:
                master_input = prompt("\n> ", completer=self._get_commands_completer()).strip()
                if not master_input:
                    continue

                parts = shlex_split(master_input)
                command, *args = parts

                if command.startswith("_"):
                    self.console.print(f"[red]noiseless: Unknown command '{command}'. See 'help' for more information.[/red]")
                    continue

                if hasattr(self, command) and callable(getattr(self, command)):
                    method = getattr(self, command)

                    # Preserve function metadata
                    @wraps(method)
                    def safe_call(*args):
                        try:
                            method(*args[: len(signature(method).parameters)])
                        except Exception as e:
                            self.console.print(f"[red]noiseless: Error: {e}[/red]")

                    safe_call(*args)
                else:
                    self.console.print(f"[red]noiseless: Unknown command '{command}'. See 'help' for more information.[/red]")

        except KeyboardInterrupt:
            self.console.print("\n[cyan]Exiting CLI. Goodbye![/cyan]")
            sys.exit(0)

    def _get_commands_completer(self) -> NestedCompleter:
        """Returns the auto-completion for known commands."""
        known_arguments = {
            "log": {
                file: {'--info': None, '--filter': self._get_directory_files_set(self.directory / "../../data/filters")}
                for file in self._get_directory_files_set(self.directory / "../../data/logs")
            },
            "help": None,
            "version": {"-v", "--verbose"},
        }

        commands = {
            method: known_arguments.get(method, None)
            for method in dir(self)
            if callable(getattr(self, method)) and not method.startswith("_")
        }

        return NestedCompleter.from_nested_dict(commands)

    def _get_directory_files_set(self, directory: Path, allow_all_option: bool = False) -> set:
        """Returns a set of file names in a directory."""
        directory.mkdir(parents=True, exist_ok=True)

        files = {f.name for f in directory.iterdir() if f.is_file() and not f.name.startswith(".")}
        if allow_all_option and files:
            files.add("all")

        return files
