export const showMessageError = (context, mensaje, titulo = '') => {
    const msg = titulo === '' ? mensaje : `<strong>${titulo}</strong><br/>${mensaje}`;
    context.$toast.error(msg);
};