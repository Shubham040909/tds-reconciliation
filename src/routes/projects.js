const express = require("express");
const multer = require("multer");

const {
  createProject,
  listProjects,
  upsertCompanyCodeMappings,
  importMasterWorkbook,
  importGlWorkbook,
  import26AsText,
  runReconciliation,
  getProjectSummary,
  getPanSummary,
  getReconRows,
  getExceptions,
  getFrontendState,
} = require("../services/project-service");
const { requireAuth } = require("../middleware/auth");
const { ensureUploadDir } = require("../services/storage-service");

const router = express.Router();

ensureUploadDir();

const upload = multer({
  dest: process.env.UPLOAD_DIR || "uploads",
});

router.use(requireAuth);

router.get("/", async (req, res, next) => {
  try {
    const projects = await listProjects();
    res.json(projects);
  } catch (error) {
    next(error);
  }
});

router.post("/", async (req, res, next) => {
  try {
    const project = await createProject(req.body);
    res.status(201).json(project);
  } catch (error) {
    next(error);
  }
});

router.post("/:projectId/company-code-map", async (req, res, next) => {
  try {
    const result = await upsertCompanyCodeMappings(req.params.projectId, req.body);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.post("/:projectId/import/master", upload.single("file"), async (req, res, next) => {
  try {
    const result = await importMasterWorkbook(req.params.projectId, req.file);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

router.post("/:projectId/import/gl", upload.single("file"), async (req, res, next) => {
  try {
    const result = await importGlWorkbook(req.params.projectId, req.file);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

router.post("/:projectId/import/26as", upload.single("file"), async (req, res, next) => {
  try {
    const result = await import26AsText(req.params.projectId, req.file);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

router.post("/:projectId/reconcile", async (req, res, next) => {
  try {
    const result = await runReconciliation(req.params.projectId, req.body);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get("/:projectId/summary", async (req, res, next) => {
  try {
    const result = await getProjectSummary(req.params.projectId);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get("/:projectId/pan-summary", async (req, res, next) => {
  try {
    const result = await getPanSummary(req.params.projectId, req.query);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get("/:projectId/recon", async (req, res, next) => {
  try {
    const result = await getReconRows(req.params.projectId, req.query);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get("/:projectId/exceptions", async (req, res, next) => {
  try {
    const result = await getExceptions(req.params.projectId);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get("/:projectId/frontend-state", async (req, res, next) => {
  try {
    const result = await getFrontendState(req.params.projectId);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
