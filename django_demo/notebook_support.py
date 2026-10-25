"""Run visible synchronous Django cells safely in event-loop-based Jupyter kernels."""
from concurrent.futures import ThreadPoolExecutor


def install_sync_magic(shell):
    """Install %%django_sync using one thread for consistent ORM connection ownership.

    The cell's normal Python code executes against the notebook's user namespace.
    Exceptions propagate to the cell. There is no automatic retry or hidden database work.
    Cell magics are not supported inside a %%django_sync cell; its body is normal Python.
    """
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="django-notebook")

    def django_sync(line, cell):
        if line.strip():
            raise ValueError("%%django_sync takes no arguments")
        compiled = compile(cell, "<django_sync cell>", "exec")
        return executor.submit(exec, compiled, shell.user_ns).result()

    shell.register_magic_function(django_sync, magic_kind="cell", magic_name="django_sync")
    return executor
