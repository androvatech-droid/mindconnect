const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { ExpressPeerServer } = require('peer');
const cors = require('cors');
const { Resend } = require('resend');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(cors());
app.use(express.json());

// ── ENV varijable (postavi na Renderu) ──
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const COUNSELLOR_PASSWORD = process.env.COUNSELLOR_PASSWORD || 'mindconnect2024';
const COUNSELLOR_EMAIL = process.env.COUNSELLOR_EMAIL || 'mindsconnect7@gmail.com';

const resend = new Resend(RESEND_API_KEY);

// PeerJS server — na istom portu kao Express
const peerServer = ExpressPeerServer(server, { path: '/' });
app.use('/peerjs', peerServer);

// ── STATE ──
let waitingUsers = []; // red čekanja korisnika
let counsellorSocket = null; // trenutno spojen savjetnik
const activeSessions = {};
const SESSION_DURATION = 45 * 60;

// ── EMAIL OBAVIJEST ──
async function notifyCounsellor(count) {
  if (!RESEND_API_KEY) return;
  try {
    await resend.emails.send({
      from: 'MindConnect <onboarding@resend.dev>',
      to: COUNSELLOR_EMAIL,
      subject: `MindConnect — ${count} korisnik${count > 1 ? 'a' : ''} čeka`,
      html: `
        <div style="font-family: Georgia, serif; max-width: 480px; margin: 0 auto; padding: 2rem; color: #1a1a1a;">
          <h2 style="font-size: 1.4rem; font-weight: 500; margin-bottom: 1rem;">MindConnect</h2>
          <p style="color: #555; line-height: 1.7;">
            ${count} korisnik${count > 1 ? 'a čeka' : ' čeka'} na sesiju.
          </p>
          <p style="margin-top: 1.5rem;">
            <a href="${process.env.FRONTEND_URL || 'https://mindconnect.onrender.com'}/counsellor.html"
               style="background: #2c6b5a; color: #fff; padding: 0.65rem 1.5rem; border-radius: 3px; text-decoration: none; font-family: sans-serif; font-size: 0.9rem;">
              Priključi se →
            </a>
          </p>
          <p style="margin-top: 2rem; font-size: 0.78rem; color: #888; font-style: italic;">
            MindConnect — automatska obavijest
          </p>
        </div>
      `
    });
    console.log('Email poslan savjetniku.');
  } catch (e) {
    console.error('Email greška:', e.message);
  }
}

// ── RUTE ──
app.get('/', (req, res) => {
  res.send('MindConnect backend is running.');
});

// Provjera lozinke za savjetnika
app.post('/counsellor-auth', (req, res) => {
  const { password } = req.body;
  if (password === COUNSELLOR_PASSWORD) {
    res.json({ ok: true });
  } else {
    res.status(401).json({ ok: false, error: 'Wrong password' });
  }
});

// Status za savjetnika — koliko čeka
app.get('/queue-status', (req, res) => {
  res.json({ waiting: waitingUsers.length });
});

// ── SOCKET.IO ──
io.on('connection', (socket) => {
  console.log('Spojen:', socket.id);

  // Korisnik traži sesiju
  socket.on('find_session', ({ peerId }) => {
    console.log(`Korisnik u redu: ${socket.id}`);

    // Dodaj u red čekanja
    waitingUsers.push({ socketId: socket.id, peerId });
    socket.emit('waiting', { position: waitingUsers.length });

    // Obavijesti savjetnika ako je spojen
    if (counsellorSocket) {
      io.to(counsellorSocket).emit('user_waiting', { count: waitingUsers.length });
    }

    // Pošalji email ako je ovo prvi korisnik u redu
    if (waitingUsers.length === 1) {
      notifyCounsellor(1);
    }
  });

  // Savjetnik se prijavljuje
  socket.on('counsellor_join', ({ peerId }) => {
    counsellorSocket = socket.id;
    console.log('Savjetnik spojen:', socket.id);

    // Javi savjetniku koliko čeka
    socket.emit('queue_update', { count: waitingUsers.length });

    if (waitingUsers.length > 0) {
      socket.emit('user_waiting', { count: waitingUsers.length });
    }
  });

  // Savjetnik prihvaća sljedećeg korisnika
  socket.on('accept_user', ({ counsellorPeerId }) => {
    if (waitingUsers.length === 0) {
      socket.emit('no_users');
      return;
    }

    const user = waitingUsers.shift();
    const sessionId = `${user.socketId}-${socket.id}`;

    // Spoji ih
    io.to(user.socketId).emit('session_found', {
      sessionId,
      partnerPeerId: counsellorPeerId,
      role: 'caller'
    });

    socket.emit('session_found', {
      sessionId,
      partnerPeerId: user.peerId,
      role: 'receiver'
    });

    // Timer
    let secondsLeft = SESSION_DURATION;
    const timer = setInterval(() => {
      secondsLeft--;
      if (secondsLeft % 60 === 0 || secondsLeft <= 60) {
        io.to(user.socketId).emit('timer_tick', { secondsLeft });
        socket.emit('timer_tick', { secondsLeft });
      }
      if (secondsLeft <= 0) {
        clearInterval(timer);
        io.to(user.socketId).emit('session_ended', { reason: 'time_up' });
        socket.emit('session_ended', { reason: 'time_up' });
        delete activeSessions[sessionId];
      }
    }, 1000);

    activeSessions[sessionId] = {
      sockets: [user.socketId, socket.id],
      timer
    };

    // Javi savjetniku koliko još čeka
    socket.emit('queue_update', { count: waitingUsers.length });
  });

  // Chat poruka
  socket.on('chat_message', ({ sessionId, text }) => {
    const session = activeSessions[sessionId];
    if (!session) return;
    session.sockets.forEach(sid => {
      if (sid !== socket.id) {
        io.to(sid).emit('chat_message', { text });
      }
    });
  });

  // Ručni završetak sesije
  socket.on('end_session', ({ sessionId }) => {
    const session = activeSessions[sessionId];
    if (!session) return;
    clearInterval(session.timer);
    session.sockets.forEach(sid => {
      if (sid !== socket.id) io.to(sid).emit('session_ended', { reason: 'partner_left' });
    });
    socket.emit('session_ended', { reason: 'you_left' });
    delete activeSessions[sessionId];
  });

  // Odspajanje
  socket.on('disconnect', () => {
    console.log('Odspojio se:', socket.id);

    // Makni iz reda čekanja
    waitingUsers = waitingUsers.filter(u => u.socketId !== socket.id);

    // Ako se savjetnik odspojio
    if (counsellorSocket === socket.id) {
      counsellorSocket = null;
      console.log('Savjetnik odspojio se.');
    }

    // Završi aktivne sesije
    for (const [sessionId, session] of Object.entries(activeSessions)) {
      if (session.sockets.includes(socket.id)) {
        clearInterval(session.timer);
        session.sockets.forEach(sid => {
          if (sid !== socket.id) io.to(sid).emit('session_ended', { reason: 'partner_disconnected' });
        });
        delete activeSessions[sessionId];
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`MindConnect backend pokrenut na portu ${PORT}`);
});
