try:
    import struct as _struct
    from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305 as _ChaCha

    _SIGMA  = b"expand 32-byte k"
    _MASK32 = 0xFFFFFFFF
    _ZERO4  = b"\x00" * 4

    def _rotl32(x: int, n: int) -> int:
        return ((x << n) | (x >> (32 - n))) & _MASK32

    def _hchacha20(key: bytes, n16: bytes) -> bytes:
        if len(key) != 32 or len(n16) != 16:
            raise ValueError("bad key/nonce length")
        st = list(_struct.unpack("<16I", _SIGMA + key + n16))

        def qr(a, b, c, d):
            st[a] = (st[a] + st[b]) & _MASK32; st[d] ^= st[a]; st[d] = _rotl32(st[d], 16)
            st[c] = (st[c] + st[d]) & _MASK32; st[b] ^= st[c]; st[b] = _rotl32(st[b], 12)
            st[a] = (st[a] + st[b]) & _MASK32; st[d] ^= st[a]; st[d] = _rotl32(st[d], 8)
            st[c] = (st[c] + st[d]) & _MASK32; st[b] ^= st[c]; st[b] = _rotl32(st[b], 7)

        for _ in range(10):
            qr(0,4,8,12); qr(1,5,9,13); qr(2,6,10,14); qr(3,7,11,15)
            qr(0,5,10,15); qr(1,6,11,12); qr(2,7,8,13); qr(3,4,9,14)

        return _struct.pack("<8I", st[0],st[1],st[2],st[3], st[12],st[13],st[14],st[15])

    class XChaCha20Poly1305:
        __slots__ = ("_key",)

        def __init__(self, key: bytes):
            if len(key) != 32:
                raise ValueError("Key must be 32 bytes")
            self._key = bytes(key)

        def encrypt(self, nonce, pt, aad):
            subkey = _hchacha20(self._key, nonce[:16])
            return _ChaCha(subkey).encrypt(_ZERO4 + nonce[16:], bytes(pt), aad)

        def decrypt(self, nonce, ct, aad):
            subkey = _hchacha20(self._key, nonce[:16])
            return _ChaCha(subkey).decrypt(_ZERO4 + nonce[16:], bytes(ct), aad)

    HAS_XCHACHA = True

except ImportError:
    HAS_XCHACHA = False

    class XChaCha20Poly1305:
        def __init__(self, *a, **kw):
            raise RuntimeError("cryptography package not installed")
