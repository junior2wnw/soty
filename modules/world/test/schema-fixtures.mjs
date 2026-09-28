/** Test-only rollback of the additive v3 projection, leaving the authoritative v2 records intact. */
export function removeDiscoveryProjection(db) {
  db.exec(`
    DROP TRIGGER IF EXISTS world_profile_directory_ai;
    DROP TRIGGER IF EXISTS world_profile_directory_au;
    DROP TRIGGER IF EXISTS world_profile_directory_ad;
    DROP TRIGGER IF EXISTS world_community_directory_ai;
    DROP TRIGGER IF EXISTS world_community_directory_au;
    DROP TRIGGER IF EXISTS world_community_directory_ad;
    DROP TRIGGER IF EXISTS world_directory_ai;
    DROP TRIGGER IF EXISTS world_directory_au;
    DROP TRIGGER IF EXISTS world_directory_ad;
    DROP TABLE IF EXISTS world_directory_fts;
    DROP TABLE IF EXISTS world_directory_counts;
    DROP TABLE IF EXISTS world_directory;
  `);
}
