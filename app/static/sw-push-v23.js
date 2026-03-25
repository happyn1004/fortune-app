const CACHE_NAME = "mysticday-push-v23";

self.addEventListener("install", event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map(key => caches.delete(key)));
    await self.skipWaiting();
  })());
});

self.addEventListener("activate", event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map(key => caches.delete(key)));
    await self.clients.claim();
  })());
});

self.addEventListener("push", event => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) { data = {}; }
  const title = data.title || "오늘의 운세 알림";
  const notificationId = data.notification_id || ("push-" + Date.now());
  const options = {
    body: data.message || "새로운 알림이 도착했습니다.",
    icon: data.icon || "/static/icon-192.png",
    badge: data.badge || "/static/icon-192.png",
    renotify: true,
    tag: String(notificationId),
    vibrate: [200, 120, 200],
    timestamp: Date.now(),
    data: {
      target_url: data.target_url || "/fortune",
      notification_id: data.notification_id || null,
    },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", event => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.target_url) || "/fortune";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then(windowClients => {
      for (const client of windowClients) {
        try {
          const url = new URL(client.url);
          if (url.pathname === targetUrl || client.url.includes(targetUrl)) {
            if ("focus" in client) return client.focus();
          }
        } catch(e) {}
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
      return null;
    })
  );
});
