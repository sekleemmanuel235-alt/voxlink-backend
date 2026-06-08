# -*- coding: utf-8 -*-
"""VoxLink Enterprise Backend v3.0 (VoxLink AI)"""
import os, hashlib, secrets, base64, uuid
from datetime import datetime, timedelta
from typing import Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Depends, File, UploadFile, Form, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Boolean, Integer, Float, ForeignKey, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./voxlink.db")
if DATABASE_URL.startswith("postgres://"): DATABASE_URL = DATABASE_URL.replace("postgres://","postgresql://",1)
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread":False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

_mem: Dict[str, str] = {}
USE_REDIS = False
_rc = None
if os.getenv("REDIS_URL"):
    try:
        import redis as _r
        _rc = _r.from_url(os.getenv("REDIS_URL"), decode_responses=True, socket_connect_timeout=2)
        _rc.ping(); USE_REDIS = True
    except: pass

def cache_set(k,v,ttl=86400):
    if USE_REDIS: _rc.setex(k,ttl,v)
    else: _mem[k]=v
def cache_get(k):
    return _rc.get(k) if USE_REDIS else _mem.get(k)
def cache_del(k):
    if USE_REDIS: _rc.delete(k)
    elif k in _mem: del _mem[k]

class UserModel(Base):
    __tablename__ = "users"
    id             = Column(String, primary_key=True)
    device_id      = Column(String, unique=True, index=True)
    contact        = Column(String, unique=True, nullable=True)
    username       = Column(String, unique=True, nullable=True)
    fullname       = Column(String, default="")
    bio            = Column(String, default="")
    avatar_url     = Column(String, nullable=True)
    system_lang    = Column(String, default="fr")
    is_verified    = Column(Boolean, default=False)
    is_premium     = Column(Boolean, default=False)
    premium_until  = Column(DateTime, nullable=True)
    voice_model_id = Column(String, nullable=True)
    created_at     = Column(DateTime, default=datetime.utcnow)
    last_seen      = Column(DateTime, default=datetime.utcnow)
    contacts       = relationship("ContactModel", foreign_keys="ContactModel.owner_id", back_populates="owner", cascade="all, delete-orphan")
    drafts         = relationship("DraftModel", back_populates="owner", cascade="all, delete-orphan")

class ContactModel(Base):
    __tablename__ = "contacts"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    owner_id   = Column(String, ForeignKey("users.id"))
    contact_id = Column(String, ForeignKey("users.id"))
    nickname   = Column(String, nullable=True)
    added_at   = Column(DateTime, default=datetime.utcnow)
    owner      = relationship("UserModel", foreign_keys=[owner_id], back_populates="contacts")
    contact    = relationship("UserModel", foreign_keys=[contact_id])

class ConversationModel(Base):
    __tablename__ = "conversations"
    id           = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    participant1 = Column(String, ForeignKey("users.id"))
    participant2 = Column(String, ForeignKey("users.id"))
    created_at   = Column(DateTime, default=datetime.utcnow)
    messages     = relationship("MessageModel", back_populates="conversation", cascade="all, delete-orphan")

class MessageModel(Base):
    __tablename__ = "messages"
    id              = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    conversation_id = Column(String, ForeignKey("conversations.id"))
    sender_id       = Column(String, ForeignKey("users.id"))
    original_text   = Column(Text)
    translated_text = Column(Text, nullable=True)
    from_lang       = Column(String, default="fr")
    to_lang         = Column(String, nullable=True)
    is_translated   = Column(Boolean, default=False)
    is_read         = Column(Boolean, default=False)
    created_at      = Column(DateTime, default=datetime.utcnow)
    conversation    = relationship("ConversationModel", back_populates="messages")

class CallLogModel(Base):
    __tablename__ = "call_logs"
    id             = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    caller_id      = Column(String, ForeignKey("users.id"))
    callee_id      = Column(String, ForeignKey("users.id"))
    duration_sec   = Column(Integer, default=0)
    was_translated = Column(Boolean, default=False)
    clone_ratio    = Column(Float, default=0.0)
    started_at     = Column(DateTime, default=datetime.utcnow)
    ended_at       = Column(DateTime, nullable=True)

class DraftModel(Base):
    __tablename__ = "drafts"
    id                  = Column(Integer, primary_key=True, autoincrement=True)
    user_id             = Column(String, ForeignKey("users.id"))
    caption             = Column(String, default="")
    music_name          = Column(String, default="none")
    volume_music        = Column(Integer, default=50)
    filter_applied      = Column(String, default="none")
    media_ratio         = Column(String, default="9:16")
    compression_quality = Column(String, default="Low")
    created_at          = Column(DateTime, default=datetime.utcnow)
    owner               = relationship("UserModel", back_populates="drafts")

Base.metadata.create_all(bind=engine)
def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY","")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY","")
REVENUECAT_API_KEY = os.getenv("REVENUECAT_API_KEY","")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v3/auth/token")

def hash_device(d): return hashlib.sha256(d.encode()).hexdigest()
def make_token(uid):
    t = secrets.token_urlsafe(32); cache_set(f"tok:{t}", uid, 604800); return t
def get_uid(token: str = Depends(oauth2_scheme)):
    uid = cache_get(f"tok:{token}")
    if not uid: raise HTTPException(401,"Session expirée")
    return uid

async def translate_text(text,fl,tl):
    if fl.lower()==tl.lower() or not OPENROUTER_API_KEY: return text
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.post("https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization":f"Bearer {OPENROUTER_API_KEY}","Content-Type":"application/json"},
                json={"model":"meta-llama/llama-3-70b-instruct","messages":[
                    {"role":"system","content":f"Translate from {fl.upper()} to {tl.upper()}. Keep tone, slang, emojis. Return ONLY translation."},
                    {"role":"user","content":text}]})
        if r.status_code==200: return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e: print(f"[Translation] {e}")
    return text

async def clone_voice_el(audio_bytes,label):
    if not ELEVENLABS_API_KEY: return None
    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.elevenlabs.io/v1/voices/add",
                headers={"xi-api-key":ELEVENLABS_API_KEY},
                data={"name":f"VoxLink_{label}_{int(datetime.utcnow().timestamp())}"},
                files={"files":("sample.wav",audio_bytes,"audio/wav")})
        if r.status_code==200: return r.json().get("voice_id")
    except Exception as e: print(f"[ElevenLabs] {e}")
    return None

async def synth_speech(text,voice_id):
    if not ELEVENLABS_API_KEY or not voice_id: return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream",
                headers={"xi-api-key":ELEVENLABS_API_KEY,"Content-Type":"application/json"},
                json={"text":text,"model_id":"eleven_multilingual_v2","voice_settings":{"stability":0.5,"similarity_boost":0.85}})
        if r.status_code==200: return r.content
    except Exception as e: print(f"[ElevenLabs TTS] {e}")
    return None

class SignalingHub:
    def __init__(self): self.sockets: Dict[str, WebSocket] = {}
    async def connect(self,uid,ws): await ws.accept(); self.sockets[uid]=ws; cache_set(f"online:{uid}","1",3600)
    def disconnect(self,uid): self.sockets.pop(uid,None); cache_set(f"online:{uid}",datetime.utcnow().isoformat(),300)
    def is_online(self,uid): return cache_get(f"online:{uid}")=="1"
    async def send(self,uid,payload):
        if uid in self.sockets:
            try: await self.sockets[uid].send_json(payload); return True
            except: pass
        return False

hub = SignalingHub()
app = FastAPI(title="VoxLink API v4.0",version="4.0.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

@app.post("/api/v3/auth/token")
async def dummy_token(): raise HTTPException(400,"Use /api/v3/auth/register")

class RegisterRequest(BaseModel):
    device_id: str; contact: Optional[str]=None; system_lang: Optional[str]="fr"; username: Optional[str]=None

@app.post("/api/v3/auth/register",status_code=201)
def register(req: RegisterRequest, db: Session=Depends(get_db)):
    h=hash_device(req.device_id)
    u=db.query(UserModel).filter(UserModel.device_id==h).first()
    if u: u.last_seen=datetime.utcnow(); db.commit(); return {"status":"authenticated","token":make_token(u.id),"user_id":u.id,"is_premium":u.is_premium,"voice_model_id":u.voice_model_id}
    uid=h[:16]; base=(req.username or f"user_{uid[:6]}").lower().replace(" ","_"); uname=base; n=1
    while db.query(UserModel).filter(UserModel.username==uname).first(): uname=f"{base}{n}"; n+=1
    nu=UserModel(id=uid,device_id=h,contact=req.contact,system_lang=req.system_lang,username=uname)
    db.add(nu); db.commit(); db.refresh(nu)
    return {"status":"registered","token":make_token(uid),"user_id":uid,"is_premium":False,"voice_model_id":None}

@app.post("/api/v3/auth/logout")
def logout(token: str=Depends(oauth2_scheme)): cache_del(f"tok:{token}"); return {"status":"ok"}

class ProfileUpdate(BaseModel):
    fullname: Optional[str]=None; bio: Optional[str]=None; username: Optional[str]=None; system_lang: Optional[str]=None

@app.get("/api/v3/profile/me")
def get_me(uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    u=db.query(UserModel).filter(UserModel.id==uid).first()
    if not u: raise HTTPException(404)
    return {"id":u.id,"username":u.username,"fullname":u.fullname,"bio":u.bio,"avatar_url":u.avatar_url,"system_lang":u.system_lang,"is_premium":u.is_premium,"is_verified":u.is_verified,"voice_model_id":u.voice_model_id,"premium_until":u.premium_until,"created_at":u.created_at}

@app.patch("/api/v3/profile/update")
def update_me(req: ProfileUpdate, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    u=db.query(UserModel).filter(UserModel.id==uid).first()
    if not u: raise HTTPException(404)
    if req.fullname is not None: u.fullname=req.fullname
    if req.bio is not None: u.bio=req.bio
    if req.system_lang is not None: u.system_lang=req.system_lang
    if req.username is not None:
        if db.query(UserModel).filter(UserModel.username==req.username,UserModel.id!=uid).first(): raise HTTPException(409,"Nom d'utilisateur déjà pris.")
        u.username=req.username
    db.commit(); return {"status":"updated"}

@app.get("/api/v3/profile/{username}")
def get_profile(username: str, db: Session=Depends(get_db)):
    u=db.query(UserModel).filter(UserModel.username==username).first()
    if not u: raise HTTPException(404,"Introuvable")
    return {"id":u.id,"username":u.username,"fullname":u.fullname,"bio":u.bio,"avatar_url":u.avatar_url,"is_verified":u.is_verified,"is_premium":u.is_premium,"system_lang":u.system_lang}

@app.get("/api/v3/contacts")
def get_contacts(uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    rows=db.query(ContactModel).filter(ContactModel.owner_id==uid).all()
    return [{"contact_id":r.contact.id,"username":r.contact.username,"fullname":r.contact.fullname,"avatar_url":r.contact.avatar_url,"system_lang":r.contact.system_lang,"is_online":hub.is_online(r.contact.id),"nickname":r.nickname,"is_verified":r.contact.is_verified,"is_premium":r.contact.is_premium} for r in rows if r.contact]

class AddContactReq(BaseModel): username: str; nickname: Optional[str]=None

@app.post("/api/v3/contacts/add",status_code=201)
def add_contact(req: AddContactReq, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    t=db.query(UserModel).filter(UserModel.username==req.username).first()
    if not t: raise HTTPException(404,"Utilisateur introuvable")
    if t.id==uid: raise HTTPException(400,"Vous ne pouvez pas vous ajouter")
    if db.query(ContactModel).filter(ContactModel.owner_id==uid,ContactModel.contact_id==t.id).first(): raise HTTPException(409,"Déjà ajouté")
    db.add(ContactModel(owner_id=uid,contact_id=t.id,nickname=req.nickname)); db.commit()
    return {"status":"added","contact_id":t.id}

@app.delete("/api/v3/contacts/{cid}")
def rm_contact(cid: str, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    r=db.query(ContactModel).filter(ContactModel.owner_id==uid,ContactModel.contact_id==cid).first()
    if not r: raise HTTPException(404)
    db.delete(r); db.commit(); return {"status":"removed"}

def get_or_create_conv(u1,u2,db):
    c=db.query(ConversationModel).filter(((ConversationModel.participant1==u1)&(ConversationModel.participant2==u2))|((ConversationModel.participant1==u2)&(ConversationModel.participant2==u1))).first()
    if not c: c=ConversationModel(participant1=u1,participant2=u2); db.add(c); db.commit(); db.refresh(c)
    return c

@app.get("/api/v3/conversations")
def list_convs(uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    convs=db.query(ConversationModel).filter((ConversationModel.participant1==uid)|(ConversationModel.participant2==uid)).all()
    result=[]
    for c in convs:
        oid=c.participant2 if c.participant1==uid else c.participant1
        other=db.query(UserModel).filter(UserModel.id==oid).first()
        lm=db.query(MessageModel).filter(MessageModel.conversation_id==c.id).order_by(MessageModel.created_at.desc()).first()
        unread=db.query(MessageModel).filter(MessageModel.conversation_id==c.id,MessageModel.sender_id!=uid,MessageModel.is_read==False).count()
        if other: result.append({"conversation_id":c.id,"contact":{"id":other.id,"username":other.username,"fullname":other.fullname,"avatar_url":other.avatar_url,"system_lang":other.system_lang,"is_online":hub.is_online(other.id)},"last_message":{"text":lm.original_text,"created_at":lm.created_at,"is_mine":lm.sender_id==uid} if lm else None,"unread_count":unread})
    return sorted(result,key=lambda x:x["last_message"]["created_at"] if x["last_message"] else datetime.min,reverse=True)

@app.get("/api/v3/conversations/{cid}/messages")
def get_messages(cid: str, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    conv=get_or_create_conv(uid,cid,db)
    msgs=db.query(MessageModel).filter(MessageModel.conversation_id==conv.id).order_by(MessageModel.created_at.asc()).all()
    for m in msgs:
        if m.sender_id!=uid and not m.is_read: m.is_read=True
    db.commit()
    return [{"id":m.id,"sender_id":m.sender_id,"original_text":m.original_text,"translated_text":m.translated_text,"is_translated":m.is_translated,"is_read":m.is_read,"created_at":m.created_at} for m in msgs]

class SendMsgReq(BaseModel): to_user_id: str; text: str; translation_active: bool=True

@app.post("/api/v3/messages/send",status_code=201)
async def send_msg(req: SendMsgReq, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    sender=db.query(UserModel).filter(UserModel.id==uid).first()
    recv=db.query(UserModel).filter(UserModel.id==req.to_user_id).first()
    if not recv: raise HTTPException(404,"Destinataire introuvable")
    translated=None
    if req.translation_active and sender and sender.system_lang!=recv.system_lang:
        translated=await translate_text(req.text,sender.system_lang,recv.system_lang)
    conv=get_or_create_conv(uid,req.to_user_id,db)
    msg=MessageModel(conversation_id=conv.id,sender_id=uid,original_text=req.text,translated_text=translated,from_lang=sender.system_lang if sender else "fr",to_lang=recv.system_lang,is_translated=translated is not None)
    db.add(msg); db.commit(); db.refresh(msg)
    await hub.send(req.to_user_id,{"type":"new_message","conversation_id":conv.id,"message":{"id":msg.id,"sender_id":uid,"text":translated or req.text,"original_text":req.text,"is_translated":msg.is_translated,"created_at":msg.created_at.isoformat()}})
    return {"status":"sent","message_id":msg.id,"translated_text":translated}

class TranslateReq(BaseModel): message: str; from_lang: str; to_lang: str

@app.post("/api/v3/translate")
async def translate_ep(req: TranslateReq, uid: str=Depends(get_uid)):
    return {"translated":await translate_text(req.message,req.from_lang,req.to_lang)}

@app.post("/api/v3/voice/clone")
async def clone_voice(audio: UploadFile=File(...), uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    u=db.query(UserModel).filter(UserModel.id==uid).first()
    if not u: raise HTTPException(404)
    if not u.is_premium: raise HTTPException(403,"Clonage vocal réservé aux membres Premium.")
    vid=await clone_voice_el(await audio.read(),u.username or uid)
    if not vid: raise HTTPException(500,"Clonage échoué.")
    u.voice_model_id=vid; db.commit(); return {"status":"cloned","voice_model_id":vid}

@app.post("/api/v3/voice/synthesize")
async def synthesize(text: str=Form(...),voice_id: str=Form(...),uid: str=Depends(get_uid)):
    ab=await synth_speech(text,voice_id)
    if not ab: raise HTTPException(500,"Synthèse échouée.")
    return JSONResponse({"audio_base64":base64.b64encode(ab).decode()})

class StartCallReq(BaseModel): callee_id: str

@app.post("/api/v3/calls/start",status_code=201)
def start_call(req: StartCallReq, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    l=CallLogModel(caller_id=uid,callee_id=req.callee_id); db.add(l); db.commit(); db.refresh(l); return {"call_id":l.id}

class EndCallReq(BaseModel): call_id: str; duration_sec: int; was_translated: bool; clone_ratio: float=0.0

@app.post("/api/v3/calls/end")
def end_call(req: EndCallReq, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    l=db.query(CallLogModel).filter(CallLogModel.id==req.call_id).first()
    if l: l.duration_sec=req.duration_sec; l.was_translated=req.was_translated; l.clone_ratio=req.clone_ratio; l.ended_at=datetime.utcnow(); db.commit()
    return {"status":"ok"}

@app.get("/api/v3/calls/history")
def call_history(uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    logs=db.query(CallLogModel).filter((CallLogModel.caller_id==uid)|(CallLogModel.callee_id==uid)).order_by(CallLogModel.started_at.desc()).limit(50).all()
    out=[]
    for l in logs:
        oid=l.callee_id if l.caller_id==uid else l.caller_id
        other=db.query(UserModel).filter(UserModel.id==oid).first()
        out.append({"call_id":l.id,"direction":"outgoing" if l.caller_id==uid else "incoming","contact":{"id":other.id,"username":other.username,"fullname":other.fullname,"avatar_url":other.avatar_url} if other else None,"duration_sec":l.duration_sec,"was_translated":l.was_translated,"clone_ratio":l.clone_ratio,"started_at":l.started_at})
    return out

class DraftCreate(BaseModel): caption: str=""; music_name: str="none"; volume_music: int=50; filter_applied: str="none"; media_ratio: str="9:16"; compression_quality: str="Low"

@app.post("/api/v3/drafts",status_code=201)
def save_draft(d: DraftCreate, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    dr=DraftModel(user_id=uid,**d.dict()); db.add(dr); db.commit(); return {"status":"saved","draft_id":dr.id}

@app.get("/api/v3/drafts")
def get_drafts(uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    return db.query(DraftModel).filter(DraftModel.user_id==uid).order_by(DraftModel.created_at.desc()).all()

@app.delete("/api/v3/drafts/{did}")
def del_draft(did: int, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    d=db.query(DraftModel).filter(DraftModel.id==did,DraftModel.user_id==uid).first()
    if not d: raise HTTPException(404)
    db.delete(d); db.commit(); return {"status":"deleted"}

class PremiumReq(BaseModel): revenuecat_user_id: str; entitlement_id: str="premium"

@app.post("/api/v3/billing/verify-premium")
async def verify_premium(req: PremiumReq, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    if not REVENUECAT_API_KEY: raise HTTPException(500,"Clé RevenueCat manquante.")
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r=await c.get(f"https://api.revenuecat.com/v1/subscribers/{req.revenuecat_user_id}",headers={"Authorization":f"Bearer {REVENUECAT_API_KEY}","Content-Type":"application/json"})
        if r.status_code!=200: raise HTTPException(402,"Vérification RevenueCat échouée.")
        ent=r.json().get("subscriber",{}).get("entitlements",{}).get(req.entitlement_id,{})
        is_active=ent.get("is_active",False); expires=ent.get("expires_date")
        u=db.query(UserModel).filter(UserModel.id==uid).first()
        if not u: raise HTTPException(404)
        u.is_premium=is_active
        if expires: u.premium_until=datetime.fromisoformat(expires.replace("Z","+00:00"))
        db.commit(); return {"is_premium":is_active,"expires_at":expires}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500,f"Erreur: {e}")

@app.get("/api/v3/billing/status")
def billing_status(uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    u=db.query(UserModel).filter(UserModel.id==uid).first()
    if not u: raise HTTPException(404)
    return {"is_premium":u.is_premium,"premium_until":u.premium_until,"voice_cloning_available":u.is_premium}

@app.get("/api/v3/search/users")
def search_users(q: str, uid: str=Depends(get_uid), db: Session=Depends(get_db)):
    if len(q)<2: return []
    users=db.query(UserModel).filter((UserModel.username.ilike(f"%{q}%"))|(UserModel.fullname.ilike(f"%{q}%"))).filter(UserModel.id!=uid).limit(20).all()
    return [{"id":u.id,"username":u.username,"fullname":u.fullname,"avatar_url":u.avatar_url,"is_verified":u.is_verified} for u in users]

@app.get("/api/v3/presence/{user_id}")
def presence(user_id: str, uid: str=Depends(get_uid)): return {"user_id":user_id,"is_online":hub.is_online(user_id)}

@app.websocket("/ws/v3/{user_id}")
async def ws_ep(websocket: WebSocket, user_id: str):
    await hub.connect(user_id, websocket)
    try:
        while True:
            data=await websocket.receive_json()
            t=data.get("type",""); to=data.get("to")
            if not to: continue
            if t in ["offer","answer","ice_candidate","call_request","call_accepted","call_rejected","call_ended","typing_start","typing_stop","chat_message"]:
                await hub.send(to,{"type":t,"from":user_id,"payload":data.get("payload")})
    except WebSocketDisconnect: hub.disconnect(user_id)

@app.get("/health")
def health(): return {"status":"ok","version":"3.0.0"}

if __name__=="__main__":
    import uvicorn; uvicorn.run("main:app",host="0.0.0.0",port=8000,reload=True)

# ── Account deletion ───────────────────────────────────────────────
@app.delete("/api/v3/account")
def delete_account(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    """Suppression irréversible du compte et de toutes les données."""
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404, "Compte introuvable")
    # Supprimer contacts, messages via cascade (relations ORM)
    db.delete(u)
    db.commit()
    # Révoquer le token
    cache_del(f"tok:{uid}")
    return {"status": "deleted", "message": "Compte supprimé définitivement."}

# ── Feed (vidéos / posts) ──────────────────────────────────────────
@app.get("/api/v3/feed")
def get_feed(uid: str = Depends(get_uid), page: int = 0, limit: int = 10):
    """
    Endpoint feed — retourne les posts à afficher dans HomeFeedView.
    En production : implémenter l'algorithme de recommandation ici.
    Pour l'instant retourne une liste vide (le feed Flutter utilise
    des cards statiques démonstratives jusqu'à l'implémentation média).
    """
    return {"posts": [], "page": page, "has_more": False}

# ── Report user ────────────────────────────────────────────────────
class ReportRequest(BaseModel):
    reported_user_id: str
    reason: str
    details: Optional[str] = None

@app.post("/api/v3/report", status_code=201)
def report_user(req: ReportRequest, uid: str = Depends(get_uid)):
    """Signalement d'un utilisateur — en prod: stocker + notifier modération."""
    print(f"[REPORT] {uid} signale {req.reported_user_id}: {req.reason}")
    return {"status": "reported", "message": "Signalement reçu. Notre équipe va examiner ce contenu."}

# ── User Preferences (notifications, privacy) ─────────────────────
class PrefsModel(Base):
    __tablename__ = "user_prefs"
    user_id               = Column(String, ForeignKey("users.id"), primary_key=True)
    notif_messages        = Column(Boolean, default=True)
    notif_calls           = Column(Boolean, default=True)
    notif_mentions        = Column(Boolean, default=True)
    notif_groups          = Column(Boolean, default=False)
    notif_marketing       = Column(Boolean, default=False)
    who_can_message       = Column(String, default="contacts")
    who_can_see_profile   = Column(String, default="everyone")
    show_online_status    = Column(Boolean, default=True)
    read_receipts         = Column(Boolean, default=True)
    auto_translate_chat   = Column(Boolean, default=True)
    auto_translate_feed   = Column(Boolean, default=True)
    default_feed_mode     = Column(String, default="subtitles")

try:
    PrefsModel.__table__.create(bind=engine, checkfirst=True)
except: pass

class PrefsUpdate(BaseModel):
    notif_messages: Optional[bool] = None
    notif_calls: Optional[bool] = None
    notif_mentions: Optional[bool] = None
    notif_groups: Optional[bool] = None
    notif_marketing: Optional[bool] = None
    who_can_message: Optional[str] = None
    who_can_see_profile: Optional[str] = None
    show_online_status: Optional[bool] = None
    read_receipts: Optional[bool] = None
    auto_translate_chat: Optional[bool] = None
    auto_translate_feed: Optional[bool] = None
    default_feed_mode: Optional[str] = None

@app.get("/api/v3/preferences")
def get_prefs(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    p = db.query(PrefsModel).filter(PrefsModel.user_id == uid).first()
    if not p:
        p = PrefsModel(user_id=uid); db.add(p); db.commit(); db.refresh(p)
    return {c.key: getattr(p, c.key) for c in PrefsModel.__table__.columns}

@app.patch("/api/v3/preferences")
def update_prefs(req: PrefsUpdate, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    p = db.query(PrefsModel).filter(PrefsModel.user_id == uid).first()
    if not p:
        p = PrefsModel(user_id=uid); db.add(p)
    for field, val in req.dict(exclude_none=True).items():
        if hasattr(p, field): setattr(p, field, val)
    db.commit()
    return {"status": "updated"}

# ── Blocked users ─────────────────────────────────────────────────
class BlockModel(Base):
    __tablename__ = "blocks"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    blocker_id  = Column(String, ForeignKey("users.id"))
    blocked_id  = Column(String, ForeignKey("users.id"))
    created_at  = Column(DateTime, default=datetime.utcnow)

try:
    BlockModel.__table__.create(bind=engine, checkfirst=True)
except: pass

class BlockReq(BaseModel): user_id: str

@app.post("/api/v3/blocks", status_code=201)
def block_user(req: BlockReq, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    if db.query(BlockModel).filter(BlockModel.blocker_id==uid, BlockModel.blocked_id==req.user_id).first():
        return {"status": "already_blocked"}
    db.add(BlockModel(blocker_id=uid, blocked_id=req.user_id))
    db.commit()
    return {"status": "blocked"}

@app.delete("/api/v3/blocks/{user_id}")
def unblock_user(user_id: str, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    b = db.query(BlockModel).filter(BlockModel.blocker_id==uid, BlockModel.blocked_id==user_id).first()
    if b: db.delete(b); db.commit()
    return {"status": "unblocked"}

@app.get("/api/v3/blocks")
def get_blocked(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    blocks = db.query(BlockModel).filter(BlockModel.blocker_id==uid).all()
    result = []
    for b in blocks:
        u = db.query(UserModel).filter(UserModel.id==b.blocked_id).first()
        if u: result.append({"user_id":u.id,"username":u.username,"fullname":u.fullname,"blocked_at":b.created_at})
    return result
