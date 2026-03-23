const RICH_PREFIX = '__OKKAZO_RICH__:';

const encodeRichChatMessage = ({ kind, payload }) => {
  const k = String(kind || '').trim();
  if (!k) throw new Error('kind is required');

  const json = JSON.stringify({ v: 1, kind: k, payload: payload ?? null });
  const b64 = Buffer.from(json, 'utf8').toString('base64');
  return `${RICH_PREFIX}${b64}`;
};

module.exports = {
  RICH_PREFIX,
  encodeRichChatMessage,
};
