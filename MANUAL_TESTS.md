# Manual test checklist

Follow these steps after launching the Streamlit app (`streamlit run app.py`) to confirm the category UI and scraper wiring:

1. **Category dropdown covers all options**
   - Open the sidebar and expand the **Category** dropdown.
   - Verify every Gumroad category is present (3D, Audio, Business & Money, Comics & Graphic Novels, Design, Drawing & Painting, Education, Fiction Books, Films, Fitness & Health, Games, Music & Sound Design, Nonfiction Books, Photography, Podcasts, Productivity, Programming & Tech, Self Improvement, Software, Worldbuilding, Writing & Publishing, Other).
2. **Indented subcategories**
   - Select "Design" and open the **Subcategory** dropdown.
   - Confirm the first option is `All Subcategories` with the remaining entries indented/bulleted (e.g., `• Icons`, `• Templates`, `• Fonts`, etc.).
3. **URL generation**
   - Pick any category/subcategory combo (e.g., "Programming & Tech" → "AI & Machine Learning").
   - Click **Scrape** and watch the terminal logs: the scraper should request a URL shaped like `https://gumroad.com/discover?category=programming-and-tech&subcategory=ai-and-machine-learning`.
4. **State reset**
   - Switch to another category (e.g., "Audio") and confirm the subcategory resets to `All Subcategories` without persisting the previous choice.
5. **End-to-end scrape**
   - Run a short scrape (e.g., max products = 10, fast mode on) and ensure results populate and can be downloaded as CSV.
