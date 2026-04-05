export default async function handler(req, res) {
  const handler = require('../../../api/admin/[...path].js');
  return handler(req, res);
}
