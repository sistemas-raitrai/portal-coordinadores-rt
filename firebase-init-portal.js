import { initializeApp } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-app.js';
import { getFirestore } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import { getAuth } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import { getStorage }     from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

export const app = initializeApp({
  apiKey: "AIzaSyAdx9nVcV-UiGER3mcz-w9BcSSIZd-t5nE",
  authDomain: "sist-op-rt.firebaseapp.com",
  projectId: "sist-op-rt",
  storageBucket: "sist-op-rt.firebasestorage.app",
  messagingSenderId: "438607695630",
  appId: "1:438607695630:web:862c4a6bea4bfdd3bfd15f"
});

export const db = getFirestore(app);
export const auth = getAuth(app);
export const storage = getStorage(app); // o: getStorage(app, 'gs://sist-op-rt.firebasestorage.app')
