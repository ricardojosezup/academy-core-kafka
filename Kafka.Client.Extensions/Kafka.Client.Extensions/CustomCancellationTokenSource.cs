using System.Threading;

namespace Kafka.Client.Extensions
{
    internal class CustomCancellationTokenSource : CancellationTokenSource
    {
        public bool IsDisposed { get; private set; }
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            IsDisposed = true;
        }
    }
}
