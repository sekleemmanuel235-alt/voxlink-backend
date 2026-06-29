# -*- coding: utf-8 -*-
"""
VoxLink Backend v4.0 — Production complète
FastAPI + PostgreSQL/SQLite + Redis + WebSocket
+ OpenRouter (Traduction) + ElevenLabs (Clonage vocal)
+ RevenueCat (Premium) + APScheduler (Tâches planifiées)
+ Webhooks RevenueCat (renouvellement / annulation automatique)
"""

import os
import json
import hashlib
import secrets
import asyncio
import base64
import uuid
import shutil
from pathlib import Path
import hmac
import hashlib as hl
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any

import httpx
from fastapi import (
    FastAPI, HTTPException, Depends, File, UploadFile,
    Form, WebSocket, WebSocketDisconnect, status, Request, BackgroundTasks
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from sqlalchemy import (
    create_engine, Column, String, Boolean, Integer,
    Float, ForeignKey, DateTime, Text, event
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ═══════════════════════════════════════════════════════════════════
# 1. BASE DE DONNÉES
# ═══════════════════════════════════════════════════════════════════
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./voxlink.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════
# 2. CACHE (Redis ou fallback mémoire)
# ═══════════════════════════════════════════════════════════════════
_mem: Dict[str, str] = {}
_rc = None
USE_REDIS = False

if os.getenv("REDIS_URL"):
    try:
        import redis as _r
        _rc = _r.from_url(
            os.getenv("REDIS_URL"),
            decode_responses=True,
            socket_connect_timeout=2
        )
        _rc.ping()
        USE_REDIS = True
        print("[Redis] ✅ Connecté")
    except Exception as e:
        print(f"[Redis] ⚠️ Fallback mémoire ({e})")


def cache_set(k: str, v: str, ttl: int = 86400):
    if USE_REDIS and _rc:
        _rc.setex(k, ttl, v)
    else:
        _mem[k] = v


def cache_get(k: str) -> Optional[str]:
    if USE_REDIS and _rc:
        return _rc.get(k)
    return _mem.get(k)


def cache_del(k: str):
    if USE_REDIS and _rc:
        _rc.delete(k)
    elif k in _mem:
        del _mem[k]


# ═══════════════════════════════════════════════════════════════════
# 3. MODÈLES ORM
# ═══════════════════════════════════════════════════════════════════
class UserModel(Base):
    __tablename__ = "users"

    id              = Column(String, primary_key=True)
    device_id       = Column(String, unique=True, index=True)
    contact         = Column(String, unique=True, nullable=True)
    username        = Column(String, unique=True, nullable=True)
    fullname        = Column(String, default="")
    bio             = Column(String, default="")
    avatar_url      = Column(String, nullable=True)
    system_lang     = Column(String, default="fr")
    is_verified     = Column(Boolean, default=False)

    # ── PREMIUM ────────────────────────────────────────────────────
    is_premium          = Column(Boolean, default=False)
    premium_until       = Column(DateTime, nullable=True)
    revenuecat_user_id  = Column(String, nullable=True)   # ID RevenueCat lié
    premium_plan        = Column(String, nullable=True)   # "monthly" | "annual"
    premium_cancelled   = Column(Boolean, default=False)  # Annulé mais encore actif

    # ── VOIX ───────────────────────────────────────────────────────
    voice_model_id  = Column(String, nullable=True)       # ElevenLabs voice ID

    # ── DATES ──────────────────────────────────────────────────────
    created_at      = Column(DateTime, default=datetime.utcnow)
    last_seen       = Column(DateTime, default=datetime.utcnow)

    # ── RELATIONS ──────────────────────────────────────────────────
    contacts        = relationship(
        "ContactModel",
        foreign_keys="ContactModel.owner_id",
        back_populates="owner",
        cascade="all, delete-orphan"
    )
    drafts          = relationship(
        "DraftModel",
        back_populates="owner",
        cascade="all, delete-orphan"
    )
    posts = relationship(
        "PostModel",
        back_populates="owner",
        cascade="all, delete-orphan"
    )

    stories = relationship(
        "StoryModel",
        back_populates="owner",
        cascade="all, delete-orphan"
    )

    creator_revenues = relationship(
        "CreatorRevenueModel",
        back_populates="owner",
        cascade="all, delete-orphan"
    )

    followers = relationship(
        "FollowModel",
        foreign_keys="FollowModel.following_id",
        cascade="all, delete-orphan"
    )

    following = relationship(
        "FollowModel",
        foreign_keys="FollowModel.follower_id",
        cascade="all, delete-orphan"
    )

    notifications = relationship(
        "NotificationModel",
        foreign_keys="NotificationModel.user_id",
        cascade="all, delete-orphan"
    )

class ContactModel(Base):
    __tablename__ = "contacts"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    owner_id    = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    contact_id  = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    nickname    = Column(String, nullable=True)
    is_blocked  = Column(Boolean, default=False)
    added_at    = Column(DateTime, default=datetime.utcnow)

    owner       = relationship("UserModel", foreign_keys=[owner_id], back_populates="contacts")
    contact     = relationship("UserModel", foreign_keys=[contact_id])


class ConversationModel(Base):
    __tablename__ = "conversations"

    id           = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    participant1 = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    participant2 = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    created_at   = Column(DateTime, default=datetime.utcnow)

    messages     = relationship(
        "MessageModel",
        back_populates="conversation",
        cascade="all, delete-orphan"
    )


class MessageModel(Base):
    __tablename__ = "messages"

    id              = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    conversation_id = Column(String, ForeignKey("conversations.id", ondelete="CASCADE"))
    sender_id       = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
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

    id              = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    caller_id       = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    callee_id       = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    duration_sec    = Column(Integer, default=0)
    was_translated  = Column(Boolean, default=False)
    clone_ratio     = Column(Float, default=0.0)
    started_at      = Column(DateTime, default=datetime.utcnow)
    ended_at        = Column(DateTime, nullable=True)


class DraftModel(Base):
    __tablename__ = "drafts"

    id                   = Column(Integer, primary_key=True, autoincrement=True)
    user_id              = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    caption              = Column(String, default="")
    music_name           = Column(String, default="none")
    volume_music         = Column(Integer, default=50)
    filter_applied       = Column(String, default="none")
    media_ratio          = Column(String, default="9:16")
    compression_quality  = Column(String, default="Low")
    created_at           = Column(DateTime, default=datetime.utcnow)

    owner                = relationship("UserModel", back_populates="drafts")


class PremiumEventModel(Base):
    """Journal de tous les événements Premium (achat, renouvellement, annulation, remboursement)."""
    __tablename__ = "premium_events"

    id              = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id         = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    event_type      = Column(String)    # purchase | renewal | cancellation | refund | expiration
    plan            = Column(String, nullable=True)
    revenuecat_id   = Column(String, nullable=True)
    raw_payload     = Column(Text, nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)

class PostModel(Base):
    __tablename__ = "posts"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    caption = Column(Text, default="")
    media_url = Column(String, nullable=False)
    media_type = Column(String, default="video")
    views = Column(Integer, default=0)
    likes_count = Column(Integer, default=0)
    comments_count = Column(Integer, default=0)
    shares_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    owner = relationship("UserModel", back_populates="posts")

class StoryModel(Base):
    __tablename__ = "stories"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    media_url = Column(String, nullable=False)
    media_type = Column(String, default="image")
    views = Column(Integer, default=0)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

    owner = relationship("UserModel", back_populates="stories")

class PostLikeModel(Base):
    __tablename__ = "post_likes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(String, ForeignKey("posts.id", ondelete="CASCADE"))
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    created_at = Column(DateTime, default=datetime.utcnow)

class PostCommentModel(Base):
    __tablename__ = "post_comments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(String, ForeignKey("posts.id", ondelete="CASCADE"))
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    comment = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class CreatorRevenueModel(Base):
    __tablename__ = "creator_revenues"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    amount_usd = Column(Float, default=0.0)
    source = Column(String, default="creator_fund")
    created_at = Column(DateTime, default=datetime.utcnow)

    owner = relationship("UserModel", back_populates="creator_revenues")

class FollowModel(Base):
    __tablename__ = "follows"

    id = Column(Integer, primary_key=True, autoincrement=True)

    follower_id = Column(
        String,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False
    )

    following_id = Column(
        String,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False
    )

    created_at = Column(DateTime, default=datetime.utcnow)

class NotificationModel(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, autoincrement=True)

    user_id = Column(
        String,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False
    )

    actor_id = Column(
        String,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False
    )

    notification_type = Column(String, nullable=False)
    reference_id = Column(String, nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)


# ═══════════════════════════════════════════════════════════════════
# 4. CONFIGURATION & SÉCURITÉ
# ═══════════════════════════════════════════════════════════════════
OPENROUTER_API_KEY      = os.getenv("OPENROUTER_API_KEY", "")
ELEVENLABS_API_KEY      = os.getenv("ELEVENLABS_API_KEY", "")
REVENUECAT_API_KEY      = os.getenv("REVENUECAT_API_KEY", "")
REVENUECAT_WEBHOOK_SECRET = os.getenv("REVENUECAT_WEBHOOK_SECRET", "")
REVENUECAT_ENTITLEMENT  = os.getenv("REVENUECAT_ENTITLEMENT", "premium")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v4/auth/token")


def hash_device(d: str) -> str:
    return hashlib.sha256(d.encode()).hexdigest()


def make_token(uid: str) -> str:
    token = secrets.token_urlsafe(32)
    cache_set(f"tok:{token}", uid, 604800)  # 7 jours
    return token


def get_uid(token: str = Depends(oauth2_scheme)) -> str:
    uid = cache_get(f"tok:{token}")
    if not uid:
        raise HTTPException(status_code=401, detail="Session expirée. Reconnectez-vous.")
    return uid


# ═══════════════════════════════════════════════════════════════════
# 5. MOTEUR TRADUCTION IA (OPENROUTER — sans limite de langue)
# ═══════════════════════════════════════════════════════════════════
async def translate_text(text: str, from_lang: str, to_lang: str) -> str:
    """
    Traduit via OpenRouter / LLaMA-3 70B.
    Supporte toutes les langues du monde (ISO 639) sans restriction :
    Wolof, Bambara, Swahili, Créole haïtien, Lingala, etc.
    """
    if from_lang.lower() == to_lang.lower() or not OPENROUTER_API_KEY:
        return text
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "meta-llama/llama-3-70b-instruct",
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                f"Translate from {from_lang.upper()} to {to_lang.upper()}. "
                                "Preserve tone, slang, emojis and emotional intent. "
                                "Return ONLY the translation, nothing else."
                            )
                        },
                        {"role": "user", "content": text}
                    ]
                }
            )
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[Translation Error] {e}")
    return text


# ═══════════════════════════════════════════════════════════════════
# 6. CLONAGE & SYNTHÈSE VOCALE (ELEVENLABS)
# ═══════════════════════════════════════════════════════════════════
async def clone_voice(audio_bytes: bytes, label: str) -> Optional[str]:
    """Crée une empreinte vocale ElevenLabs. Retourne le voice_id."""
    if not ELEVENLABS_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post(
                "https://api.elevenlabs.io/v1/voices/add",
                headers={"xi-api-key": ELEVENLABS_API_KEY},
                data={"name": f"VoxLink_{label}_{int(datetime.utcnow().timestamp())}"},
                files={"files": ("sample.wav", audio_bytes, "audio/wav")}
            )
        if r.status_code == 200:
            return r.json().get("voice_id")
    except Exception as e:
        print(f"[ElevenLabs Clone Error] {e}")
    return None


async def synthesize_voice(text: str, voice_id: str) -> Optional[bytes]:
    """Synthétise du texte avec la voix clonée. Retourne l'audio MP3."""
    if not ELEVENLABS_API_KEY or not voice_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=12.0) as c:
            r = await c.post(
                f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream",
                headers={
                    "xi-api-key": ELEVENLABS_API_KEY,
                    "Content-Type": "application/json"
                },
                json={
                    "text": text,
                    "model_id": "eleven_multilingual_v2",
                    "voice_settings": {"stability": 0.5, "similarity_boost": 0.85}
                }
            )
        if r.status_code == 200:
            return r.content
    except Exception as e:
        print(f"[ElevenLabs TTS Error] {e}")
    return None


# ═══════════════════════════════════════════════════════════════════
# 7. SIGNALISATION WEBSOCKET
# ═══════════════════════════════════════════════════════════════════
class SignalingHub:
    def __init__(self):
        self.sockets: Dict[str, WebSocket] = {}

    async def connect(self, uid: str, ws: WebSocket):
        await ws.accept()
        self.sockets[uid] = ws
        cache_set(f"online:{uid}", "1", 3600)

    def disconnect(self, uid: str):
        self.sockets.pop(uid, None)
        cache_set(f"online:{uid}", datetime.utcnow().isoformat(), 300)

    def is_online(self, uid: str) -> bool:
        return cache_get(f"online:{uid}") == "1"

    async def send(self, uid: str, payload: dict) -> bool:
        if uid in self.sockets:
            try:
                await self.sockets[uid].send_json(payload)
                return True
            except Exception:
                pass
        return False


hub = SignalingHub()


# ═══════════════════════════════════════════════════════════════════
# 8. LOGIQUE PREMIUM — FONCTIONS CENTRALES
# ═══════════════════════════════════════════════════════════════════
async def verify_revenuecat_subscription(
    revenuecat_user_id: str,
    db: Session,
    user: UserModel
) -> dict:
    """
    Vérifie le statut Premium via l'API REST RevenueCat.
    Met à jour la base de données et retourne le statut.
    Appelé après chaque achat ET à chaque démarrage de l'app.
    """
    if not REVENUECAT_API_KEY:
        raise HTTPException(500, "Clé RevenueCat non configurée sur le serveur.")

    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(
                f"https://api.revenuecat.com/v1/subscribers/{revenuecat_user_id}",
                headers={
                    "Authorization": f"Bearer {REVENUECAT_API_KEY}",
                    "Content-Type": "application/json"
                }
            )

        if r.status_code != 200:
            raise HTTPException(402, "Impossible de vérifier l'abonnement RevenueCat.")

        data         = r.json()
        subscriber   = data.get("subscriber", {})
        entitlements = subscriber.get("entitlements", {})
        premium_ent  = entitlements.get(REVENUECAT_ENTITLEMENT, {})

        is_active   = premium_ent.get("is_active", False)
        expires_str = premium_ent.get("expires_date")
        product_id  = premium_ent.get("product_identifier", "")

        # Déterminer le plan (mensuel / annuel)
        plan = "annual" if "annual" in product_id.lower() else "monthly"

        # Mettre à jour en base
        user.is_premium         = is_active
        user.revenuecat_user_id = revenuecat_user_id
        user.premium_plan       = plan if is_active else None
        user.premium_cancelled  = False

        if expires_str:
            try:
                user.premium_until = datetime.fromisoformat(
                    expires_str.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                pass

        db.commit()

        return {
            "is_premium":   is_active,
            "plan":         plan if is_active else None,
            "expires_at":   expires_str,
            "product_id":   product_id
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur RevenueCat : {str(e)}")


def _log_premium_event(
    db: Session,
    user_id: str,
    event_type: str,
    plan: Optional[str] = None,
    rc_id: Optional[str] = None,
    payload: Optional[str] = None
):
    """Enregistre chaque événement Premium dans le journal d'audit."""
    db.add(PremiumEventModel(
        user_id=user_id,
        event_type=event_type,
        plan=plan,
        revenuecat_user_id=rc_id,
        raw_payload=payload
    ))
    db.commit()


# ═══════════════════════════════════════════════════════════════════
# 9. TÂCHE PLANIFIÉE — RÉVOCATION AUTOMATIQUE À EXPIRATION
# ═══════════════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler()


@scheduler.scheduled_job("cron", hour=2, minute=0)
def revoke_expired_premium():
    """
    Tourne chaque nuit à 2h00 UTC.
    Révoque le Premium de tous les utilisateurs dont l'abonnement a expiré.
    """
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        expired_users = db.query(UserModel).filter(
            UserModel.is_premium == True,
            UserModel.premium_until != None,
            UserModel.premium_until < now
        ).all()

        count = 0
        for u in expired_users:
            u.is_premium    = False
            u.premium_plan  = None
            _log_premium_event(
                db, u.id,
                event_type="expiration",
                plan=u.premium_plan
            )
            count += 1

        db.commit()
        if count > 0:
            print(f"[Scheduler] ✅ {count} abonnements Premium révoqués (expirés)")
    except Exception as e:
        print(f"[Scheduler] ❌ Erreur révocation : {e}")
    finally:
        db.close()


@scheduler.scheduled_job("cron", hour=3, minute=0)
async def sync_premium_with_revenuecat():
    """
    Tourne chaque nuit à 3h00 UTC.
    Synchronise le statut Premium avec RevenueCat pour tous les abonnés actifs.
    Détecte les annulations et remboursements silencieux.
    """
    if not REVENUECAT_API_KEY:
        return

    db = SessionLocal()
    try:
        users_with_rc = db.query(UserModel).filter(
            UserModel.revenuecat_user_id != None,
            UserModel.is_premium == True
        ).all()

        for u in users_with_rc:
            try:
                await verify_revenuecat_subscription(u.revenuecat_user_id, db, u)
                await asyncio.sleep(0.2)  # Éviter le rate limiting RevenueCat
            except Exception:
                pass

        print(f"[Scheduler] ✅ Sync RevenueCat terminée ({len(users_with_rc)} users)")
    except Exception as e:
        print(f"[Scheduler] ❌ Erreur sync : {e}")
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════
# 10. APPLICATION FASTAPI
# ═══════════════════════════════════════════════════════════════════
app = FastAPI(
    title="VoxLink API v4.0",
    description="Backend production — Traduction IA + Clonage Vocal + Premium RevenueCat",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    scheduler.start()
    print("[VoxLink] ✅ Serveur démarré — Scheduler actif")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ─── HEALTH ────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "4.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "scheduler": scheduler.running
    }


# ═══════════════════════════════════════════════════════════════════
# 11. AUTH
# ═══════════════════════════════════════════════════════════════════
@app.post("/api/v4/auth/token")
async def dummy_token():
    raise HTTPException(400, "Utilisez /api/v4/auth/register avec votre device_id.")


class RegisterRequest(BaseModel):
    device_id:   str
    contact:     Optional[str] = None
    system_lang: Optional[str] = "fr"
    username:    Optional[str] = None


@app.post("/api/v4/auth/register", status_code=201)
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    h = hash_device(req.device_id)
    u = db.query(UserModel).filter(UserModel.device_id == h).first()

    if u:
        u.last_seen = datetime.utcnow()
        db.commit()
        return {
            "status": "authenticated",
            "token": make_token(u.id),
            "user_id": u.id,
            "is_premium": u.is_premium,
            "voice_model_id": u.voice_model_id
        }

    uid = h[:16]
    base = (req.username or f"user_{uid[:6]}").lower().replace(" ", "_")
    uname = base
    n = 1
    while db.query(UserModel).filter(UserModel.username == uname).first():
        uname = f"{base}{n}"
        n += 1

    new_u = UserModel(
        id=uid, device_id=h,
        contact=req.contact,
        system_lang=req.system_lang,
        username=uname
    )
    db.add(new_u)
    db.commit()
    db.refresh(new_u)

    return {
        "status": "registered",
        "token": make_token(uid),
        "user_id": uid,
        "is_premium": False,
        "voice_model_id": None
    }


@app.post("/api/v4/auth/logout")
def logout(token: str = Depends(oauth2_scheme)):
    cache_del(f"tok:{token}")
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════
# 12. PROFIL
# ═══════════════════════════════════════════════════════════════════
class ProfileUpdate(BaseModel):
    fullname:    Optional[str] = None
    bio:         Optional[str] = None
    username:    Optional[str] = None
    system_lang: Optional[str] = None


@app.get("/api/v4/profile/me")
def get_me(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404)
    return {
        "id": u.id, "username": u.username, "fullname": u.fullname,
        "bio": u.bio, "avatar_url": u.avatar_url, "system_lang": u.system_lang,
        "is_premium": u.is_premium, "is_verified": u.is_verified,
        "voice_model_id": u.voice_model_id, "premium_until": u.premium_until,
        "premium_plan": u.premium_plan, "premium_cancelled": u.premium_cancelled,
        "created_at": u.created_at
    }


@app.patch("/api/v4/profile/update")
def update_me(
    req: ProfileUpdate,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404)
    if req.fullname   is not None: u.fullname   = req.fullname
    if req.bio        is not None: u.bio        = req.bio
    if req.system_lang is not None: u.system_lang = req.system_lang
    if req.username   is not None:
        ex = db.query(UserModel).filter(
            UserModel.username == req.username,
            UserModel.id != uid
        ).first()
        if ex:
            raise HTTPException(409, "Nom d'utilisateur déjà pris.")
        u.username = req.username
    db.commit()
    return {"status": "updated"}


@app.get("/api/v4/profile/{username}")
def get_profile(
    username: str,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    u = db.query(UserModel).filter(UserModel.username == username).first()
    if not u:
        raise HTTPException(404, "Utilisateur introuvable")

    followers_count = db.query(FollowModel).filter(
        FollowModel.following_id == u.id
    ).count()

    following_count = db.query(FollowModel).filter(
        FollowModel.follower_id == u.id
    ).count()

    is_following = db.query(FollowModel).filter(
        FollowModel.follower_id == uid,
        FollowModel.following_id == u.id
    ).first() is not None

    return {
        "id": u.id,
        "username": u.username,
        "fullname": u.fullname,
        "bio": u.bio,
        "avatar_url": u.avatar_url,
        "is_verified": u.is_verified,
        "is_premium": u.is_premium,
        "system_lang": u.system_lang,
        "followers_count": followers_count,
        "following_count": following_count,
        "is_following": is_following
    }

@app.post("/api/v4/follow/{user_id}")
def follow_user(
    user_id: str,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    if uid == user_id:
        raise HTTPException(400, "Vous ne pouvez pas vous suivre vous-même.")

    target = db.query(UserModel).filter(UserModel.id == user_id).first()
    if not target:
        raise HTTPException(404, "Utilisateur introuvable.")

    existing = db.query(FollowModel).filter(
        FollowModel.follower_id == uid,
        FollowModel.following_id == user_id
    ).first()

    if existing:
        return {"status": "already_following"}

    db.add(FollowModel(
        follower_id=uid,
        following_id=user_id
    ))
    db.commit()

    return {"status": "following"}


@app.post("/api/v4/unfollow/{user_id}")
def unfollow_user(
    user_id: str,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    follow = db.query(FollowModel).filter(
        FollowModel.follower_id == uid,
        FollowModel.following_id == user_id
    ).first()

    if not follow:
        return {"status": "not_following"}

    db.delete(follow)
    db.commit()

    return {"status": "unfollowed"}

# ═══════════════════════════════════════════════════════════════════
# 13. CONTACTS (avec blocage)
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/v4/contacts")
def get_contacts(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    rows = db.query(ContactModel).filter(
        ContactModel.owner_id == uid,
        ContactModel.is_blocked == False
    ).all()
    return [
        {
            "contact_id": r.contact.id,
            "username": r.contact.username,
            "fullname": r.contact.fullname,
            "avatar_url": r.contact.avatar_url,
            "system_lang": r.contact.system_lang,
            "is_online": hub.is_online(r.contact.id),
            "nickname": r.nickname,
            "is_verified": r.contact.is_verified,
            "is_premium": r.contact.is_premium
        }
        for r in rows if r.contact
    ]


class AddContactReq(BaseModel):
    username: str
    nickname: Optional[str] = None


@app.post("/api/v4/contacts/add", status_code=201)
def add_contact(
    req: AddContactReq,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    t = db.query(UserModel).filter(UserModel.username == req.username).first()
    if not t:
        raise HTTPException(404, "Utilisateur introuvable")
    if t.id == uid:
        raise HTTPException(400, "Vous ne pouvez pas vous ajouter vous-même")
    ex = db.query(ContactModel).filter(
        ContactModel.owner_id == uid,
        ContactModel.contact_id == t.id
    ).first()
    if ex:
        if ex.is_blocked:
            ex.is_blocked = False
            db.commit()
            return {"status": "unblocked_and_added"}
        raise HTTPException(409, "Contact déjà ajouté")
    db.add(ContactModel(owner_id=uid, contact_id=t.id, nickname=req.nickname))
    db.commit()
    return {"status": "added", "contact_id": t.id}


@app.delete("/api/v4/contacts/{cid}")
def remove_contact(cid: str, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    r = db.query(ContactModel).filter(
        ContactModel.owner_id == uid,
        ContactModel.contact_id == cid
    ).first()
    if not r:
        raise HTTPException(404)
    db.delete(r)
    db.commit()
    return {"status": "removed"}


# ── Blocage ────────────────────────────────────────────────────────
@app.post("/api/v4/block/{user_id}")
def block_user(user_id: str, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    row = db.query(ContactModel).filter(
        ContactModel.owner_id == uid,
        ContactModel.contact_id == user_id
    ).first()
    if row:
        row.is_blocked = True
    else:
        db.add(ContactModel(owner_id=uid, contact_id=user_id, is_blocked=True))
    db.commit()
    return {"status": "blocked"}


@app.post("/api/v4/unblock/{user_id}")
def unblock_user(user_id: str, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    row = db.query(ContactModel).filter(
        ContactModel.owner_id == uid,
        ContactModel.contact_id == user_id
    ).first()
    if row:
        row.is_blocked = False
        db.commit()
    return {"status": "unblocked"}


@app.get("/api/v4/blocked")
def get_blocked_users(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    rows = db.query(ContactModel).filter(
        ContactModel.owner_id == uid,
        ContactModel.is_blocked == True
    ).all()
    return [
        {
            "user_id": r.contact.id,
            "username": r.contact.username,
            "fullname": r.contact.fullname
        }
        for r in rows if r.contact
    ]


# ═══════════════════════════════════════════════════════════════════
# 14. CONVERSATIONS & MESSAGES
# ═══════════════════════════════════════════════════════════════════
def get_or_create_conv(uid1: str, uid2: str, db: Session) -> ConversationModel:
    c = db.query(ConversationModel).filter(
        ((ConversationModel.participant1 == uid1) & (ConversationModel.participant2 == uid2)) |
        ((ConversationModel.participant1 == uid2) & (ConversationModel.participant2 == uid1))
    ).first()
    if not c:
        c = ConversationModel(participant1=uid1, participant2=uid2)
        db.add(c)
        db.commit()
        db.refresh(c)
    return c


@app.get("/api/v4/conversations")
def list_convs(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    convs = db.query(ConversationModel).filter(
        (ConversationModel.participant1 == uid) | (ConversationModel.participant2 == uid)
    ).all()
    result = []
    for c in convs:
        oid = c.participant2 if c.participant1 == uid else c.participant1
        other = db.query(UserModel).filter(UserModel.id == oid).first()
        lm = db.query(MessageModel).filter(
            MessageModel.conversation_id == c.id
        ).order_by(MessageModel.created_at.desc()).first()
        unread = db.query(MessageModel).filter(
            MessageModel.conversation_id == c.id,
            MessageModel.sender_id != uid,
            MessageModel.is_read == False
        ).count()
        if other:
            result.append({
                "conversation_id": c.id,
                "contact": {
                    "id": other.id, "username": other.username,
                    "fullname": other.fullname, "avatar_url": other.avatar_url,
                    "system_lang": other.system_lang,
                    "is_online": hub.is_online(other.id)
                },
                "last_message": {
                    "text": lm.original_text,
                    "created_at": lm.created_at,
                    "is_mine": lm.sender_id == uid
                } if lm else None,
                "unread_count": unread
            })
    return sorted(
        result,
        key=lambda x: x["last_message"]["created_at"] if x["last_message"] else datetime.min,
        reverse=True
    )


@app.get("/api/v4/conversations/{cid}/messages")
def get_messages(cid: str, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    conv = get_or_create_conv(uid, cid, db)
    msgs = db.query(MessageModel).filter(
        MessageModel.conversation_id == conv.id
    ).order_by(MessageModel.created_at.asc()).all()
    for m in msgs:
        if m.sender_id != uid and not m.is_read:
            m.is_read = True
    db.commit()
    return [
        {
            "id": m.id, "sender_id": m.sender_id,
            "original_text": m.original_text,
            "translated_text": m.translated_text,
            "is_translated": m.is_translated,
            "is_read": m.is_read,
            "created_at": m.created_at
        }
        for m in msgs
    ]


class SendMsgReq(BaseModel):
    to_user_id:         str
    text:               str
    translation_active: bool = True


@app.post("/api/v4/messages/send", status_code=201)
async def send_message(
    req: SendMsgReq,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    sender = db.query(UserModel).filter(UserModel.id == uid).first()
    recv   = db.query(UserModel).filter(UserModel.id == req.to_user_id).first()
    if not recv:
        raise HTTPException(404, "Destinataire introuvable")

    translated = None
    if req.translation_active and sender and sender.system_lang != recv.system_lang:
        translated = await translate_text(req.text, sender.system_lang, recv.system_lang)

    conv = get_or_create_conv(uid, req.to_user_id, db)
    msg = MessageModel(
        conversation_id=conv.id, sender_id=uid,
        original_text=req.text,
        translated_text=translated,
        from_lang=sender.system_lang if sender else "fr",
        to_lang=recv.system_lang,
        is_translated=translated is not None
    )
    db.add(msg)
    db.commit()
    db.refresh(msg)

    await hub.send(req.to_user_id, {
        "type": "new_message",
        "conversation_id": conv.id,
        "message": {
            "id": msg.id, "sender_id": uid,
            "text": translated or req.text,
            "original_text": req.text,
            "is_translated": msg.is_translated,
            "created_at": msg.created_at.isoformat()
        }
    })

    return {
        "status": "sent",
        "message_id": msg.id,
        "translated_text": translated
    }


# ─── Traduction standalone ─────────────────────────────────────────
class TranslateReq(BaseModel):
    message:   str
    from_lang: str
    to_lang:   str


@app.post("/api/v4/translate")
async def translate_ep(req: TranslateReq, uid: str = Depends(get_uid)):
    result = await translate_text(req.message, req.from_lang, req.to_lang)
    return {"translated": result}


# ═══════════════════════════════════════════════════════════════════
# 15. CLONAGE VOCAL (PREMIUM UNIQUEMENT)
# ═══════════════════════════════════════════════════════════════════
@app.post("/api/v4/voice/clone")
async def clone_voice_endpoint(
    audio: UploadFile = File(...),
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404)
    if not u.is_premium:
        raise HTTPException(
            403,
            "Le clonage vocal est réservé aux membres Premium. "
            "Abonnez-vous depuis l'onglet Profil."
        )

    audio_bytes = await audio.read()
    voice_id = await clone_voice(audio_bytes, u.username or uid)
    if not voice_id:
        raise HTTPException(
            500,
            "Clonage vocal échoué. Vérifiez votre clé ElevenLabs dans Railway."
        )

    u.voice_model_id = voice_id
    db.commit()
    return {"status": "cloned", "voice_model_id": voice_id}


@app.post("/api/v4/voice/synthesize")
async def synthesize_endpoint(
    text:     str = Form(...),
    voice_id: str = Form(...),
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u or not u.is_premium:
        raise HTTPException(403, "Synthèse vocale réservée aux membres Premium.")

    audio_bytes = await synthesize_voice(text, voice_id)
    if not audio_bytes:
        raise HTTPException(500, "Synthèse vocale échouée.")

    return JSONResponse({"audio_base64": base64.b64encode(audio_bytes).decode()})


# ═══════════════════════════════════════════════════════════════════
# 16. APPELS
# ═══════════════════════════════════════════════════════════════════
class CreatePostResponse(BaseModel):
    id: str
    media_url: str
    caption: str

class StartCallReq(BaseModel):
    callee_id: str

@app.post("/api/v4/posts/create", response_model=CreatePostResponse)
async def create_post(
    caption: str = Form(""),
    media: UploadFile = File(...),
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    ext = Path(media.filename).suffix.lower()
    filename = f"{uuid.uuid4()}{ext}"

    save_path = Path("uploads/posts") / filename

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(media.file, buffer)

    post = PostModel(
        user_id=uid,
        caption=caption,
        media_url=f"/uploads/posts/{filename}",
        media_type="video" if ext in [".mp4", ".mov", ".mkv"] else "image"
    )

    db.add(post)
    db.commit()
    db.refresh(post)

    return CreatePostResponse(
        id=post.id,
        media_url=post.media_url,
        caption=post.caption
    )

@app.get("/uploads/posts/{filename}")
def get_post_file(filename: str):
    file_path = Path("uploads/posts") / filename

    if not file_path.exists():
        raise HTTPException(404, "Fichier introuvable")

    return FileResponse(str(file_path))

@app.get("/api/v4/posts/feed")
def get_feed(
    limit: int = 20,
    db: Session = Depends(get_db)
):
    posts = (
        db.query(PostModel)
        .order_by(PostModel.created_at.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": p.id,
            "user_id": p.user_id,
            "caption": p.caption,
            "media_url": p.media_url,
            "media_type": p.media_type,
            "views": p.views,
            "likes_count": p.likes_count,
            "comments_count": p.comments_count,
            "shares_count": p.shares_count,
            "created_at": p.created_at.isoformat()
        }
        for p in posts
    ]

@app.post("/api/v4/posts/{post_id}/like")
def like_post(
    post_id: str,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    post = db.query(PostModel).filter(PostModel.id == post_id).first()
    if not post:
        raise HTTPException(404, "Post introuvable")

    existing = (
        db.query(PostLikeModel)
        .filter(
            PostLikeModel.post_id == post_id,
            PostLikeModel.user_id == uid
        )
        .first()
    )

    if existing:
        return {"status": "already_liked"}

    db.add(
        PostLikeModel(
            post_id=post_id,
            user_id=uid
        )
    )

    post.likes_count += 1
    db.commit()

    return {
        "status": "liked",
        "likes_count": post.likes_count
    }

class AddCommentReq(BaseModel):
    comment: str


@app.post("/api/v4/posts/{post_id}/comment")
def comment_post(
    post_id: str,
    req: AddCommentReq,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    post = db.query(PostModel).filter(PostModel.id == post_id).first()

    if not post:
        raise HTTPException(404, "Post introuvable")

    db.add(
        PostCommentModel(
            post_id=post_id,
            user_id=uid,
            comment=req.comment
        )
    )

    post.comments_count += 1

    db.commit()

    return {
        "status": "comment_added",
        "comments_count": post.comments_count
    }

class CreateStoryResponse(BaseModel):
    id: str
    media_url: str


@app.post("/api/v4/stories/create", response_model=CreateStoryResponse)
async def create_story(
    media: UploadFile = File(...),
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    ext = Path(media.filename).suffix.lower()
    filename = f"{uuid.uuid4()}{ext}"

    save_path = Path("uploads/stories") / filename

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(media.file, buffer)

    story = StoryModel(
        user_id=uid,
        media_url=f"/uploads/stories/{filename}",
        media_type="video" if ext in [".mp4", ".mov", ".avi"] else "image",
        expires_at=datetime.utcnow() + timedelta(hours=24)
    )

    db.add(story)
    db.commit()
    db.refresh(story)

    return CreateStoryResponse(
        id=story.id,
        media_url=story.media_url
    )

@app.get("/api/v4/stories/feed")
def get_stories(
    db: Session = Depends(get_db)
):
    stories = (
        db.query(StoryModel)
        .filter(StoryModel.expires_at > datetime.utcnow())
        .all()
    )

    return [
        {
            "id": s.id,
            "user_id": s.user_id,
            "media_url": s.media_url,
            "media_type": s.media_type,
            "views": s.views,
            "expires_at": s.expires_at.isoformat()
        }
        for s in stories
    ]

@app.post("/api/v4/calls/start", status_code=201)
def start_call(
    req: StartCallReq,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    log = CallLogModel(caller_id=uid, callee_id=req.callee_id)
    db.add(log)
    db.commit()
    db.refresh(log)
    return {"call_id": log.id}


class EndCallReq(BaseModel):
    call_id:        str
    duration_sec:   int
    was_translated: bool
    clone_ratio:    float = 0.0


@app.post("/api/v4/calls/end")
def end_call(
    req: EndCallReq,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    log = db.query(CallLogModel).filter(CallLogModel.id == req.call_id).first()
    if log:
        log.duration_sec   = req.duration_sec
        log.was_translated = req.was_translated
        log.clone_ratio    = req.clone_ratio
        log.ended_at       = datetime.utcnow()
        db.commit()
    return {"status": "ok"}


@app.get("/api/v4/calls/history")
def call_history(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    logs = db.query(CallLogModel).filter(
        (CallLogModel.caller_id == uid) | (CallLogModel.callee_id == uid)
    ).order_by(CallLogModel.started_at.desc()).limit(50).all()

    result = []
    for l in logs:
        oid   = l.callee_id if l.caller_id == uid else l.caller_id
        other = db.query(UserModel).filter(UserModel.id == oid).first()
        result.append({
            "call_id":      l.id,
            "direction":    "outgoing" if l.caller_id == uid else "incoming",
            "contact": {
                "id": other.id, "username": other.username,
                "fullname": other.fullname, "avatar_url": other.avatar_url
            } if other else None,
            "duration_sec":   l.duration_sec,
            "was_translated": l.was_translated,
            "clone_ratio":    l.clone_ratio,
            "started_at":     l.started_at
        })
    return result


# ═══════════════════════════════════════════════════════════════════
# 17. PREMIUM — ENDPOINTS COMPLETS
# ═══════════════════════════════════════════════════════════════════

# ── Vérification après achat (appelé par Flutter après purchasePackage) ──
class PremiumVerifyReq(BaseModel):
    revenuecat_user_id: str
    entitlement_id:     str = "premium"


@app.post("/api/v4/billing/verify-premium")
async def verify_premium_endpoint(
    req: PremiumVerifyReq,
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    """
    Appelé par Flutter immédiatement après un achat RevenueCat.
    Vérifie côté serveur et active le Premium.
    """
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404)

    result = await verify_revenuecat_subscription(req.revenuecat_user_id, db, u)

    if result["is_premium"]:
        _log_premium_event(
            db, uid, "purchase",
            plan=result.get("plan"),
            rc_id=req.revenuecat_user_id
        )

    return result


# ── Statut billing ─────────────────────────────────────────────────
@app.get("/api/v4/billing/status")
def billing_status(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404)
    return {
        "is_premium":              u.is_premium,
        "premium_until":           u.premium_until,
        "premium_plan":            u.premium_plan,
        "premium_cancelled":       u.premium_cancelled,
        "voice_cloning_available": u.is_premium and u.voice_model_id is not None
    }


# ── Webhook RevenueCat (renouvellement / annulation / remboursement) ──
@app.post("/api/v4/webhooks/revenuecat")
async def revenuecat_webhook(request: Request, db: Session = Depends(get_db)):
    """
    RevenueCat notifie ce endpoint automatiquement pour :
    - INITIAL_PURCHASE    : Premier achat
    - RENEWAL             : Renouvellement mensuel/annuel automatique
    - CANCELLATION        : L'utilisateur annule (abonnement reste actif jusqu'à expiration)
    - EXPIRATION          : L'abonnement expire après annulation
    - REFUND              : Remboursement Apple/Google
    - BILLING_ISSUE       : Problème de paiement (carte expirée etc.)
    - PRODUCT_CHANGE      : Changement mensuel → annuel ou inversement

    Configuration dans RevenueCat :
    Dashboard → Project → Integrations → Webhooks
    → URL : https://VOTRE_URL.up.railway.app/api/v4/webhooks/revenuecat
    → Authorization : votre REVENUECAT_WEBHOOK_SECRET
    """
    # Vérifier la signature du webhook (sécurité)
    if REVENUECAT_WEBHOOK_SECRET:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != REVENUECAT_WEBHOOK_SECRET:
            raise HTTPException(401, "Webhook signature invalide")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "Payload JSON invalide")

    event = payload.get("event", {})
    event_type  = event.get("type", "")
    rc_user_id  = event.get("app_user_id", "")
    product_id  = event.get("product_id", "")
    expiry_str  = event.get("expiration_at_ms")

    # Trouver l'utilisateur VoxLink via son ID RevenueCat
    user = db.query(UserModel).filter(
        UserModel.revenuecat_user_id == rc_user_id
    ).first()

    if not user:
        # Peut arriver si l'utilisateur n'a jamais ouvert l'app après l'achat
        print(f"[Webhook] Utilisateur RevenueCat inconnu : {rc_user_id}")
        return {"status": "user_not_found"}

    plan = "annual" if "annual" in product_id.lower() else "monthly"

    if event_type in ("INITIAL_PURCHASE", "RENEWAL", "PRODUCT_CHANGE"):
        user.is_premium       = True
        user.premium_plan     = plan
        user.premium_cancelled = False
        if expiry_str:
            user.premium_until = datetime.utcfromtimestamp(expiry_str / 1000)

    elif event_type == "CANCELLATION":
        # Annulé mais reste actif jusqu'à expiration
        user.premium_cancelled = True
        if expiry_str:
            user.premium_until = datetime.utcfromtimestamp(expiry_str / 1000)

    elif event_type in ("EXPIRATION", "REFUND", "BILLING_ISSUE"):
        user.is_premium        = False
        user.premium_plan      = None
        user.premium_cancelled = False

    db.commit()

    _log_premium_event(
        db, user.id,
        event_type=event_type.lower(),
        plan=plan,
        rc_id=rc_user_id,
        payload=json.dumps(event)
    )

    # Notifier l'app en temps réel si l'utilisateur est connecté
    await hub.send(user.id, {
        "type":       "premium_status_changed",
        "is_premium": user.is_premium,
        "event":      event_type
    })

    return {"status": "processed", "event": event_type}


# ── Journal Premium (admin) ────────────────────────────────────────
@app.get("/api/v4/billing/events")
def premium_events(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    """Historique des événements Premium de l'utilisateur."""
    events = db.query(PremiumEventModel).filter(
        PremiumEventModel.user_id == uid
    ).order_by(PremiumEventModel.created_at.desc()).limit(20).all()
    return [
        {
            "event_type": e.event_type,
            "plan":       e.plan,
            "created_at": e.created_at
        }
        for e in events
    ]


# ═══════════════════════════════════════════════════════════════════
# 18. BROUILLONS (Studio créateur)
# ═══════════════════════════════════════════════════════════════════
class DraftCreate(BaseModel):
    caption:             str = ""
    music_name:          str = "none"
    volume_music:        int = 50
    filter_applied:      str = "none"
    media_ratio:         str = "9:16"
    compression_quality: str = "Low"


@app.post("/api/v4/drafts", status_code=201)
def save_draft(d: DraftCreate, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    dr = DraftModel(user_id=uid, **d.dict())
    db.add(dr)
    db.commit()
    return {"status": "saved", "draft_id": dr.id}


@app.get("/api/v4/drafts")
def get_drafts(uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    return db.query(DraftModel).filter(
        DraftModel.user_id == uid
    ).order_by(DraftModel.created_at.desc()).all()


@app.delete("/api/v4/drafts/{did}")
def del_draft(did: int, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    d = db.query(DraftModel).filter(
        DraftModel.id == did,
        DraftModel.user_id == uid
    ).first()
    if not d:
        raise HTTPException(404)
    db.delete(d)
    db.commit()
    return {"status": "deleted"}


# ═══════════════════════════════════════════════════════════════════
# 19. RECHERCHE & PRÉSENCE
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/v4/search/users")
def search_users(q: str, uid: str = Depends(get_uid), db: Session = Depends(get_db)):
    if len(q) < 2:
        return []
    users = db.query(UserModel).filter(
        (UserModel.username.ilike(f"%{q}%")) | (UserModel.fullname.ilike(f"%{q}%"))
    ).filter(UserModel.id != uid).limit(20).all()
    return [
        {
            "id": u.id, "username": u.username,
            "fullname": u.fullname, "avatar_url": u.avatar_url,
            "is_verified": u.is_verified, "is_premium": u.is_premium
        }
        for u in users
    ]


@app.get("/api/v4/presence/{user_id}")
def presence(user_id: str, uid: str = Depends(get_uid)):
    return {"user_id": user_id, "is_online": hub.is_online(user_id)}


# ═══════════════════════════════════════════════════════════════════
# 20. SIGNALEMENT
# ═══════════════════════════════════════════════════════════════════
class ReportReq(BaseModel):
    reported_user_id: str
    reason:           str
    details:          Optional[str] = None


@app.post("/api/v4/report", status_code=201)
def report_user(req: ReportReq, uid: str = Depends(get_uid)):
    print(f"[REPORT] {uid} → {req.reported_user_id} : {req.reason}")
    return {"status": "reported"}


# ═══════════════════════════════════════════════════════════════════
# 21. SUPPRESSION DE COMPTE
# ═══════════════════════════════════════════════════════════════════
@app.delete("/api/v4/account")
def delete_account(
    token: str = Depends(oauth2_scheme),
    uid: str = Depends(get_uid),
    db: Session = Depends(get_db)
):
    """Suppression définitive et irréversible du compte et de toutes les données."""
    u = db.query(UserModel).filter(UserModel.id == uid).first()
    if not u:
        raise HTTPException(404)
    db.delete(u)
    db.commit()
    cache_del(f"tok:{token}")
    return {"status": "deleted", "message": "Compte supprimé définitivement."}


# ═══════════════════════════════════════════════════════════════════
# 22. WEBSOCKET UNIFIÉ
# ═══════════════════════════════════════════════════════════════════
@app.websocket("/ws/v4/{user_id}")
async def ws_endpoint(websocket: WebSocket, user_id: str):
    await hub.connect(user_id, websocket)
    try:
        while True:
            data = await websocket.receive_json()
            t  = data.get("type", "")
            to = data.get("to")
            if not to:
                continue
            if t in [
                "offer", "answer", "ice_candidate",
                "call_request", "call_accepted", "call_rejected", "call_ended",
                "typing_start", "typing_stop", "chat_message"
            ]:
                await hub.send(to, {
                    "type":    t,
                    "from":    user_id,
                    "payload": data.get("payload")
                })
    except WebSocketDisconnect:
        hub.disconnect(user_id)


# ═══════════════════════════════════════════════════════════════════
# 23. POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
