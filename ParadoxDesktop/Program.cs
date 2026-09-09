using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace ParadoxDesktop
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string[] args)
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.SetUnhandledExceptionMode(UnhandledExceptionMode.CatchException);

            Application.ThreadException += Application_ThreadException;
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            var mainForm = new ParadoxDesktopMainForm();

            if (args.Length == 1 && System.IO.File.Exists(args[0]))
            {
                string ext = System.IO.Path.GetExtension(args[0]);
                if (string.Equals(ext, ".db", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(ext, ".sql", StringComparison.OrdinalIgnoreCase))
                {
                    mainForm.Load += (s, e) => mainForm.OpenPath(args[0]);
                }
            }

            Application.Run(mainForm);
        }

        private static void Application_ThreadException(object sender, System.Threading.ThreadExceptionEventArgs e)
        {
            ShowUnhandledException(e.Exception);
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            ShowUnhandledException(e.ExceptionObject as Exception);
        }

        private static void ShowUnhandledException(Exception ex)
        {
            string message = ex != null ? ex.ToString() : "An unknown error occurred.";

            try
            {
                MessageBox.Show(message, "Unhandled Error",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            catch
            {
                // If showing the message box itself fails, there's nothing more we can do.
            }
        }
    }
}
