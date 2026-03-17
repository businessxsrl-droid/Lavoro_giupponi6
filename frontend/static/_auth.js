/**
 * Calor Systems _auth.js
 * Gestisce i JWT Token (Access & Refresh) salvandoli in locale.
 * Sostituisce la fetch globale per iniettare automaticamente il Bearer in ogni chiamata /api/
 * ed effettuare il refresh invisibile in caso di 401.
 */

const Auth = {
    setTokens(access, refresh) {
        localStorage.setItem('access_token', access);
        localStorage.setItem('refresh_token', refresh);
    },

    getTokens() {
        return {
            access: localStorage.getItem('access_token'),
            refresh: localStorage.getItem('refresh_token')
        };
    },

    clear() {
        localStorage.removeItem('access_token');
        localStorage.removeItem('refresh_token');
        window.location.href = '/login';
    },

    async refreshToken() {
        const { refresh } = this.getTokens();
        if (!refresh) return false;

        try {
            const res = await fetch('/api/auth/refresh', {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${refresh}` }
            });
            if (res.ok) {
                const data = await res.json();
                localStorage.setItem('access_token', data.access_token);
                return true;
            }
        } catch (e) { console.error('Refresh token errato:', e); }

        return false;
    }
};

// 1. Controllo immediato: se siamo nella dashboard ma manca l'access token, vai al login
if (window.location.pathname !== '/login' && !Auth.getTokens().access) {
    Auth.clear();
}

// 2. Interceptor sulle chiamate API HTTP
const originalFetch = window.fetch;
window.fetch = async (...args) => {
    let [resource, config] = args;

    // Ignora le non-api o auth (login/refresh)
    if (typeof resource === 'string' && resource.startsWith('/api/') && !resource.startsWith('/api/auth/')) {
        config = config || {};
        config.headers = config.headers || {};

        let { access } = Auth.getTokens();
        if (access) {
            // Per il FormData (upload) non resettiamo il Content-Type, lasciamo che il browser aggiunga il boundary
            if (!(config.body instanceof FormData)) {
                config.headers['Content-Type'] = config.headers['Content-Type'] || 'application/json';
            }
            config.headers['Authorization'] = `Bearer ${access}`;
        }

        let response = await originalFetch(resource, config);

        // Se 401 Unauthorized e non volevamo già rinfrescare, proviamo il silent refresh
        if (response.status === 401) {
            const refreshed = await Auth.refreshToken();
            if (refreshed) {
                // Riprova con il nuovo token estratto
                access = Auth.getTokens().access;
                config.headers['Authorization'] = `Bearer ${access}`;
                response = await originalFetch(resource, config);
            } else {
                Auth.clear(); // Fallimento refresh -> espelli
            }
        }
        return response;
    }

    return originalFetch(resource, config);
};
