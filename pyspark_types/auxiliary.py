from typing import Type

class LongT(int):
    def __repr__(self):
        return f"LongT({super().__repr__()})"

class ShortT(int):
    def __repr__(self):
        return f"ShortT({super().__repr__()})"

class ByteT(int):
    def __repr__(self):
        return f"ByteT({super().__repr__()})"

class BoundDecimal:
    """
    Custom data type that represents a decimal with a specific scale and precision.
    """

    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        return f"BoundDecimal(precision={self.precision}, scale={self.scale})"


def create_bound_decimal_type(precision: int, scale: int) -> Type[BoundDecimal]:
    """
    Factory method that creates a new BoundDecimal type with the specified precision and scale.
    """
    class _BoundDecimal(BoundDecimal):
        pass

    _BoundDecimal.__name__ = f"BoundDecimal_{precision}_{scale}"
    _BoundDecimal.precision = precision
    _BoundDecimal.scale = scale

    return _BoundDecimal
