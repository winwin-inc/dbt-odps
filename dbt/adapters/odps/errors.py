class NotTableError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__()
        self.code = code
        self.message = message
