import ftplib
import os
import socket


# FTP上传工具类
def FtpConnect(HOST, USER, PASSWD):
    try:
        ftp = ftplib.FTP(HOST)
    except (socket.error, socket.gaierror) as e:
        print('Error, cannot reach ' + HOST)
        return
    else:
        print('Connect To Host Success...')

    try:
        ftp.login(USER, PASSWD)
    except ftplib.error_perm:
        print('Username or Passwd Error')
        ftp.quit()
        return
    else:
        print('Login Success...')

    return ftp


def FtpDownload(ftp, remotepath, localpath):
    try:
        ftp.retrbinary('RETR %s' % remotepath, open(localpath, 'wb').write)
    except ftplib.error_perm:
        print('File Error')
        os.unlink(localpath)
    else:
        print('Download Success...')
    ftp.quit()


def FtpUpload(ftp, remotepath, localpath):
    try:
        ftp.storbinary('STOR %s' % remotepath, open(localpath, 'rb'))
    except ftplib.error_perm:
        print('File Error')
        os.unlink(localpath)
    else:
        print('Upload Success...')
    ftp.quit()


def FtpDeleteAll(ftp):
    try:
        for fileList in ftp.nlst():
            ftp.delete(fileList)
    except ftplib.error_perm:
        print('File Error')
    else:
        print('Delete All File  Success...')


if __name__ == '__main__':
    ftp = FtpConnect('192.168.10.216', 'ftptest10', '987654321')
    # FtpDownload(ftp, './test10.conf', '222.rar')  # 下载
    # FtpUpload(ftp, './test1.txt', '')  # 上传
    # FtpDelete(ftp,"test1.zip")
    # FtpDeleteAll(ftp)                             # 删除所有文件
