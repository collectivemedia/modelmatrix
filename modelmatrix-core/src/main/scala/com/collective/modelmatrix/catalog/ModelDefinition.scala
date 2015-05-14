package com.collective.modelmatrix.catalog

import java.time.Instant

case class ModelDefinition(
  id: Int,
  source: String,
  createdBy: String,
  createdAt: Instant
)
