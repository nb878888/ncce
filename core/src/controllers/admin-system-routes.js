function registerAdminSystemRoutes({
  app,
  store,
  logger,
  requireAdminToken,
  requireAdminRole,
  requireSuperAdminRole,
  requireDangerConfirmation,
  getDefaultSystemConfig,
  getRuntimeConfig,
  updateRuntimeConfig,
}) {
  app.get("/api/super-admin-announcement", (req, res) => {
    try {
      res.json({ ok: true, data: store.getSuperAdminAnnouncement() });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  app.post(
    "/api/super-admin/announcement",
    requireAdminToken,
    requireSuperAdminRole,
    (req, res) => {
      try {
        if (
          !requireDangerConfirmation(
            req,
            res,
            "UPDATE_SUPER_ADMIN_ANNOUNCEMENT",
          )
        ) {
          return;
        }

        const { content, password } = req.body;
        const data = store.setSuperAdminAnnouncement(content, password);
        logger.warn("更新超级管理员公告", {
          admin: req.currentUser?.username || "",
          hasContent: !!String(content || "").trim(),
          hasPassword: !!String(password || "").trim(),
          confirmation: "UPDATE_SUPER_ADMIN_ANNOUNCEMENT",
        });
        res.json({ ok: true, data });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );

  app.post("/api/super-admin-announcement/verify", (req, res) => {
    try {
      const { password } = req.body;
      const valid = store.verifySuperAdminAnnouncementPassword(password);
      res.json({ ok: true, valid });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  app.get("/api/announcement", requireAdminToken, (req, res) => {
    try {
      const announcement = { ...store.getAnnouncement() };
      announcement.shouldShow = store.shouldShowAnnouncement(
        req.currentUser?.username,
      );
      res.json({ ok: true, data: announcement });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  app.post("/api/announcement/read", requireAdminToken, (req, res) => {
    try {
      if (req.currentUser?.username) {
        store.markAnnouncementRead(req.currentUser.username);
      }
      res.json({ ok: true });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  app.post(
    "/api/admin/announcement",
    requireAdminToken,
    requireAdminRole,
    (req, res) => {
      try {
        if (!requireDangerConfirmation(req, res, "UPDATE_ANNOUNCEMENT")) return;
        const { content, showOnce } = req.body || {};
        const data = store.setAnnouncement(content, showOnce);
        logger.warn("更新系统公告", {
          admin: req.currentUser?.username || "",
          hasContent: !!String(content || "").trim(),
          showOnce: showOnce !== false,
          confirmation: "UPDATE_ANNOUNCEMENT",
        });
        res.json({ ok: true, data });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );

  app.get(
    "/api/admin/system-config",
    requireAdminToken,
    requireAdminRole,
    (req, res) => {
      try {
        res.json({
          ok: true,
          data: {
            saved: store.getSystemConfig(),
            default: getDefaultSystemConfig(),
            current: getRuntimeConfig(),
          },
        });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );

  app.post(
    "/api/admin/system-config",
    requireAdminToken,
    requireAdminRole,
    (req, res) => {
      try {
        if (!requireDangerConfirmation(req, res, "UPDATE_SYSTEM_CONFIG")) return;
        const { serverUrl, clientVersion, platform, os } = req.body || {};
        const saved = store.setSystemConfig({
          serverUrl,
          clientVersion,
          platform,
          os,
        });
        updateRuntimeConfig(saved);
        logger.warn("更新系统配置", {
          admin: req.currentUser?.username || "",
          serverUrl: saved?.serverUrl || "",
          clientVersion: saved?.clientVersion || "",
          platform: saved?.platform || "",
          os: saved?.os || "",
          confirmation: "UPDATE_SYSTEM_CONFIG",
        });
        res.json({
          ok: true,
          data: {
            saved,
            current: getRuntimeConfig(),
          },
        });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );

  app.post(
    "/api/admin/system-config/reset",
    requireAdminToken,
    requireAdminRole,
    (req, res) => {
      try {
        if (!requireDangerConfirmation(req, res, "RESET_SYSTEM_CONFIG")) return;
        const saved = getDefaultSystemConfig();
        store.setSystemConfig(saved);
        updateRuntimeConfig(saved);
        logger.warn("重置系统配置", {
          admin: req.currentUser?.username || "",
          confirmation: "RESET_SYSTEM_CONFIG",
        });
        res.json({
          ok: true,
          data: {
            saved,
            current: getRuntimeConfig(),
          },
        });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );

  app.get(
    "/api/admin/wx-config",
    requireAdminToken,
    requireAdminRole,
    (req, res) => {
      try {
        res.json({ ok: true, data: store.getGlobalWxConfig() });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );

  app.post(
    "/api/admin/wx-config",
    requireAdminToken,
    requireAdminRole,
    (req, res) => {
      try {
        if (!requireDangerConfirmation(req, res, "UPDATE_WX_CONFIG")) return;
        const data = store.setGlobalWxConfig(req.body || {});
        logger.warn("更新微信配置", {
          admin: req.currentUser?.username || "",
          enabled: data?.enabled === true,
          autoAddAccount: data?.autoAddAccount === true,
          userIsolation: data?.userIsolation === true,
          apiBase: data?.apiBase || "",
          confirmation: "UPDATE_WX_CONFIG",
        });
        res.json({ ok: true, data });
      } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
      }
    },
  );
}

module.exports = { registerAdminSystemRoutes };
