$ErrorActionPreference='Stop'
$D=Join-Path $env:USERPROFILE 'xver-gui-build'
New-Item -Path $D -ItemType Directory -Force|Out-Null
Invoke-WebRequest -Uri 'https://files.manuscdn.com/user_upload_by_module/session_file/310519663926035155/QiCDlCoLudJHgKON.py' -OutFile (Join-Path $D 'main.py')
Invoke-WebRequest -Uri 'https://s.imgz.io/2026/10/22/9e48c78d8db39b24af2432d5e061006ca34b1aeebe6366bf.jpg' -OutFile (Join-Path $D 'background.jpg')
$L=@'
import os,sys,runpy,traceback,threading,queue,io,contextlib,tkinter as tk
from tkinter import ttk,messagebox
from PIL import Image,ImageTk,ImageEnhance
BASE=getattr(sys,'_MEIPASS',os.path.dirname(os.path.abspath(__file__)))
APP=os.path.dirname(sys.executable) if getattr(sys,'frozen',False) else os.path.dirname(os.path.abspath(__file__))
class Writer:
    def __init__(self,q): self.q=q
    def write(self,s):
        if s.strip(): self.q.put(s.rstrip())
    def flush(self): pass
class App:
    def __init__(self):
        self.q=queue.Queue();self.running=False
        self.root=tk.Tk();self.root.title('XVER TrueMoney Bot');self.root.geometry('900x600');self.root.minsize(760,500)
        try:self.root.iconname('XVER Bot')
        except:pass
        img=Image.open(os.path.join(BASE,'background.jpg')).convert('RGB');img.thumbnail((1200,800));img=ImageEnhance.Brightness(img).enhance(.42);self.bg=ImageTk.PhotoImage(img)
        self.canvas=tk.Canvas(self.root,highlightthickness=0);self.canvas.pack(fill='both',expand=True);self.canvas.create_image(0,0,image=self.bg,anchor='nw')
        self.panel=tk.Frame(self.canvas,bg='#101522',bd=0);self.canvas.create_window(450,300,window=self.panel,width=640,height=460)
        tk.Label(self.panel,text='XVER BOT',font=('Segoe UI',28,'bold'),fg='#72d6ff',bg='#101522').pack(pady=(25,2))
        tk.Label(self.panel,text='TrueMoney capture service',font=('Segoe UI',11),fg='#aab7c4',bg='#101522').pack(pady=(0,20))
        tk.Label(self.panel,text='Discord Token',font=('Segoe UI',11,'bold'),fg='white',bg='#101522',anchor='w').pack(fill='x',padx=42)
        self.token=tk.Entry(self.panel,show='•',font=('Segoe UI',12),bg='#202b3b',fg='white',insertbackground='white',relief='flat')
        self.token.pack(fill='x',padx=42,pady=(7,16),ipady=10)
        self.start=ttk.Button(self.panel,text='เริ่มบอท',command=self.start_bot);self.start.pack(pady=4,ipadx=22,ipady=7)
        self.status=tk.Label(self.panel,text='พร้อมทำงาน',font=('Segoe UI',11),fg='#77e39b',bg='#101522');self.status.pack(pady=8)
        self.out=tk.Text(self.panel,height=10,bg='#0a0e16',fg='#d6e2ee',insertbackground='white',relief='flat',font=('Consolas',9));self.out.pack(fill='both',expand=True,padx=28,pady=(4,22))
        self.root.after(150,self.poll);self.root.protocol('WM_DELETE_WINDOW',self.close)
    def log(self,s): self.out.insert('end',s+'\n');self.out.see('end')
    def start_bot(self):
        if self.running:return
        t=self.token.get().strip()
        if not t:return messagebox.showwarning('ต้องใส่ Token','กรุณาใส่ Discord Token ก่อนเริ่มบอท')
        os.environ['DISCORD_BOT_TOKEN']=t;os.environ.setdefault('DB_PATH',os.path.join(APP,'bot_users.db'));self.running=True;self.start.config(state='disabled');self.token.config(state='disabled');self.status.config(text='กำลังเริ่มบอท...',fg='#ffd166');threading.Thread(target=self.worker,daemon=True).start()
    def worker(self):
        try:
            w=Writer(self.q)
            with contextlib.redirect_stdout(w),contextlib.redirect_stderr(w):
                runpy.run_path(os.path.join(BASE,'main.py'),run_name='__main__')
        except BaseException:
            self.q.put(traceback.format_exc())
        finally:self.q.put('__DONE__')
    def poll(self):
        try:
            while True:
                s=self.q.get_nowait()
                if s=='__DONE__':self.status.config(text='บอทหยุดทำงานหรือเกิด Error',fg='#ff7777');self.running=False;self.start.config(state='normal');self.token.config(state='normal')
                else:self.log(s);self.status.config(text='บอทกำลังทำงาน',fg='#77e39b')
        except queue.Empty:pass
        self.root.after(150,self.poll)
    def close(self):
        if self.running and not messagebox.askyesno('ปิดโปรแกรม','บอทยังทำงานอยู่ ต้องการปิดหรือไม่?'):return
        self.root.destroy()
App().root.mainloop()
'@
Set-Content (Join-Path $D 'gui_launcher.py') $L -Encoding UTF8
py -3 -m venv (Join-Path $D 'venv')
$V=Join-Path $D 'venv\Scripts\python.exe'
& $V -m pip install -q --upgrade pip pyinstaller discord.py telethon aiohttp Pillow zxing-cpp primp python-dotenv pynacl davey requests certifi truststore
Set-Location $D
$PI=Join-Path $D 'venv\Scripts\pyinstaller.exe'
& $PI --noconfirm --clean --onefile --windowed --name xver-main --add-data 'main.py;.' --add-data 'background.jpg;.' --collect-all aiohttp --collect-all discord --collect-all telethon --collect-all zxingcpp --collect-all primp --hidden-import PIL --hidden-import PIL.Image --hidden-import nacl gui_launcher.py
Copy-Item (Join-Path $D 'dist\xver-main.exe') (Join-Path $env:USERPROFILE 'Desktop\xver-main.exe') -Force
Write-Host "สร้างเสร็จ: $env:USERPROFILE\Desktop\xver-main.exe"
