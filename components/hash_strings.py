import hashlib


class MagOilPassword:
    def __init__(self, login: str = 'rwtarif', password: str = 'rwtarif123', salt: str = None, auth_key: str = None):
        self.login = login
        self.password = password
        self.salt = salt
        self.auth_key = auth_key

    @staticmethod
    def e(string: str):
        return hashlib.md5(string.encode('utf-8')).hexdigest()

    def t(self, t: str):
        return self.e(":)" + t + "8(")[16:]

    def crypt_password(self):
        return self.e(self.e(self.e(self.t(self.password)) + self.salt) + self.auth_key)
