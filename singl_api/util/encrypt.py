import base64
import json
import gzip

class MyEncoder(json.JSONEncoder):
    def my_encoder(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def encrypt_rf(enstr):
    """加密方法：base64"""
    bytes_enstr = bytes(enstr, encoding="utf8")
    test_enstr = base64.b64encode(bytes_enstr)
    str_enstr = MyEncoder().my_encoder(test_enstr)

    return str_enstr

def encrypt_decode(enstr):
    """解密方法：base64"""
    bytes_enstr = bytes(enstr, encoding="utf8")
    # print('bytes_enstr:',bytes_enstr)
    test_enstr = base64.b64decode(bytes_enstr)
    # print('test_enstr',test_enstr)
    # print(type(test_enstr))
    str_enstr = MyEncoder().my_encoder(test_enstr)
    # print('str_enstr',str_enstr)
    return str_enstr

def parameter_ungzip(parameters):
    # parameters = 'H4sIAAAAAAAAAI2Sv27CMBDG3yULSwRB3UAIqaI8AeqCGExiqKXENraTFlXdqqZL26GCpUMZWLr0jyoVtSCepgnwFnUChChRCRkc+/Td+XffuXmpYGBBpaRwYFETNvoUKqqiAwG7hPVl3AFMBhjscKXUjKtaqmLADrBNcQpMKcQEB6kOMG0YSDfnNmSY2KaJ5J4SxDnB61SuM0QFkseSsnJdfzTxh9/e7KFMwiivBgXUKF3dJl+pCeY6A3pYJ5M7UibZtbxWjLPLcxjYxLV8sBY1+W3/2n9NLO7dqIOwjBoUkUtRDRPXq1Y2IIXYILi6szQf3l+p5ILOc/FGiS2oLU5wF+G940nouCAMdNOjOjOkPGoXS3uTvfhD93f6tficLqbPZQNx0DahURVM6lNYdcIsILKxIl1nvUlSUcB6NhTxOexCOnfSjvuDd//u1R/N4s9mk6MGGTFWeEERg0YDWXsdjMuSgEfaftO88Yc/uPXGL5IrbVMNCMChqCGWbVVMm4Q4Jw7EhR5YCzOmGPqzerpZTt68+XWa6VjechDQVpikKQiLHgiznD967s+WpPUHtt+Lu3oEAAA='
    # 1. 按照UTF-8 编码格式进行编码，转化成bytes类型b'abdc'
    str_encode = parameters.encode('utf-8')
    # print(str_encode, type(str_encode))
    # 2. 解密，返回byte编码串
    par = base64.b64decode(str_encode)
    # print(par)
    # 3. gzip 解压缩
    no_g = gzip.decompress(par)
    # print(no_g)
    # 4. 将解压缩后的byte内容转化成str取出并返回
    par_use = str(no_g, encoding='utf-8')
    return par_use

# res = parameter_ungzip('H4sIAAAAAAAAAIuOBQApu0wNAgAAAA==')
# print(res)
# if res:
#     print('OK')
# else:
#     print('N')
